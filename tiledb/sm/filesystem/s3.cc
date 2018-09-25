/**
 * @file   s3.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file implements the S3 class.
 */

#ifdef HAVE_S3

#include <boost/interprocess/streams/bufferstream.hpp>
#include <fstream>
#include <iostream>
#include <regex>
#include <vector>

#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/misc/executor.h"

#ifdef _WIN32
#include <Windows.h>
#endif

#include "tiledb/sm/filesystem/s3.h"
#include "tiledb/sm/filesystem/Chrono.h"

namespace tiledb {
namespace sm {

namespace {

/**
 * Return the exception name and error message from the given outcome object.
 *
 * @tparam R AWS result type
 * @tparam E AWS error type
 * @param outcome Outcome to retrieve error message from
 * @return Error message string
 */
template <typename R, typename E>
std::string outcome_error_message(const Aws::Utils::Outcome<R, E>& outcome) {
  return std::string("\nException:  ") +
         outcome.GetError().GetExceptionName().c_str() +
         std::string("\nError message:  ") +
         outcome.GetError().GetMessage().c_str();
}

}  // namespace

/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Ensures that the AWS library is only initialized once per process. */
static std::once_flag aws_lib_initialized;

static int op = 0;
static std::string ID(int o, long t) { char buf[20]; sprintf(buf, "%04d %05ld ", o, t); return buf;}

#define START int _o = op++; ChronoCpu _ch("S3"); _ch.tic();
#define LOG std::cout << ID(_o, since())
#define DONE " DONE: " << (_ch.tac(), _ch.getLastTime_us() / 1000.0) << std::endl;


long S3::since() const {
  timespec tac_time;
  (void) clock_gettime(CLOCK_REALTIME, &tac_time);

  float elapsed_s = (float)(tac_time.tv_sec - tic_time_.tv_sec);
  float elapsed_ns = (float)(tac_time.tv_nsec - tic_time_.tv_nsec);
  return (elapsed_s * 1e3f + elapsed_ns / 1e6f);
}

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

S3::S3() {
  client_ = nullptr;
  max_parallel_ops_ = 1;
  multipart_part_size_ = 0;

  (void) clock_gettime(CLOCK_REALTIME, &tic_time_);
}

S3::~S3() {
  for (auto& buff : file_buffers_)
    delete buff.second;

  for (auto& part : parts_)
    delete part.second;
}

/* ********************************* */
/*                 API               */
/* ********************************* */

Status S3::init(const Config::S3Params& s3_config, ThreadPool* thread_pool) {
  if (thread_pool == nullptr) {
    return LOG_STATUS(
        Status::S3Error("Can't initialize with null thread pool."));
  }
  START
  LOG << "S3::init" << std::endl;

  // Initialize the library once per process.
  std::call_once(aws_lib_initialized, [this]() { Aws::InitAPI(options_); });

  vfs_thread_pool_ = thread_pool;
  max_parallel_ops_ = s3_config.max_parallel_ops_;
  multipart_part_size_ = s3_config.multipart_part_size_;
  file_buffer_size_ = multipart_part_size_ * max_parallel_ops_;
  region_ = s3_config.region_;

  Aws::Client::ClientConfiguration config;
  if (!s3_config.region_.empty())
    config.region = s3_config.region_.c_str();
  if (!s3_config.endpoint_override_.empty())
    config.endpointOverride = s3_config.endpoint_override_.c_str();

  if (!s3_config.proxy_host_.empty()) {
    config.proxyHost = s3_config.proxy_host_.c_str();
    config.proxyPort = s3_config.proxy_port_;
    config.proxyScheme = s3_config.proxy_scheme_ == "http" ?
                             Aws::Http::Scheme::HTTP :
                             Aws::Http::Scheme::HTTPS;
    config.proxyUserName = s3_config.proxy_username_.c_str();
    config.proxyPassword = s3_config.proxy_password_.c_str();
  }

  config.scheme = (s3_config.scheme_ == "http") ? Aws::Http::Scheme::HTTP :
                                                  Aws::Http::Scheme::HTTPS;
  config.connectTimeoutMs = s3_config.connect_timeout_ms_;
  config.requestTimeoutMs = s3_config.request_timeout_ms_;

  config.retryStrategy = Aws::MakeShared<Aws::Client::DefaultRetryStrategy>(
      constants::s3_allocation_tag.c_str(),
      s3_config.connect_max_tries_,
      s3_config.connect_scale_factor_);

  // Connect S3 client
  client_ = Aws::MakeShared<Aws::S3::S3Client>(
      constants::s3_allocation_tag.c_str(),
      config,
      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
      s3_config.use_virtual_addressing_);

  LOG << "S3::init" << DONE;

  return Status::Ok();
}

Status S3::create_bucket(const URI& bucket) const {
  if (!bucket.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + bucket.to_string())));
  }

  START
  LOG << "S3::create_bucket: " << bucket.c_str() << std::endl;


  Aws::Http::URI aws_uri = bucket.c_str();
  Aws::S3::Model::CreateBucketRequest create_bucket_request;
  create_bucket_request.SetBucket(aws_uri.GetAuthority());

  // Set the bucket location constraint equal to the S3 region.
  // Note: empty string and 'us-east-1' are parsing errors in the SDK.
  if (!region_.empty() && region_ != "us-east-1") {
    Aws::S3::Model::CreateBucketConfiguration cfg;
    Aws::String region_str(region_.c_str());
    auto location_constraint = Aws::S3::Model::BucketLocationConstraintMapper::
        GetBucketLocationConstraintForName(region_str);
    cfg.SetLocationConstraint(location_constraint);
    create_bucket_request.SetCreateBucketConfiguration(cfg);
  }

  auto create_bucket_outcome = client_->CreateBucket(create_bucket_request);
  if (!create_bucket_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to create S3 bucket ") + bucket.to_string() +
        outcome_error_message(create_bucket_outcome)));
  }

  if (!wait_for_bucket_to_be_created(bucket)) {
    return LOG_STATUS(Status::S3Error(
        "Failed waiting for bucket " + bucket.to_string() + " to be created."));
  }

  LOG << "S3::create_bucket" << DONE;

  return Status::Ok();
}

Status S3::remove_bucket(const URI& bucket) const {

  START
  LOG << "S3::remove_bucket: " << bucket.c_str() << std::endl;
  
  // Empty bucket
  RETURN_NOT_OK(empty_bucket(bucket));

  // Delete bucket
  Aws::Http::URI aws_uri = bucket.c_str();
  Aws::S3::Model::DeleteBucketRequest delete_bucket_request;
  delete_bucket_request.SetBucket(aws_uri.GetAuthority());
  auto delete_bucket_outcome = client_->DeleteBucket(delete_bucket_request);
  if (!delete_bucket_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to remove S3 bucket ") + bucket.to_string() +
        outcome_error_message(delete_bucket_outcome)));
  }
  
  LOG << "S3::remove_bucket" << DONE;

  return Status::Ok();
}

Status S3::disconnect() {
  START
  LOG << "S3::disconnect" << std::endl;

  for (const auto& record : multipart_upload_request_) {
    auto completed_multipart_upload = multipart_upload_[record.first];
    auto complete_multipart_upload_request = record.second;
    complete_multipart_upload_request.WithMultipartUpload(
        completed_multipart_upload);
    auto complete_multipart_upload_outcome =
        client_->CompleteMultipartUpload(complete_multipart_upload_request);
    if (!complete_multipart_upload_outcome.IsSuccess()) {
      return LOG_STATUS(Status::S3Error(
          std::string("Failed to disconnect and flush S3 objects. ") +
          outcome_error_message(complete_multipart_upload_outcome)));
    }
  }
  Aws::ShutdownAPI(options_);

  LOG << "S3::disconnect" << DONE;

  return Status::Ok();
}

Status S3::empty_bucket(const URI& bucket) const {
  START
  LOG << "S3::empty_bucket: " << bucket.c_str() << std::endl;
  
  auto uri_dir = bucket.add_trailing_slash();
  return remove_dir(uri_dir);

  LOG << "S3::empty_bucket" << DONE;
}


Status S3::flush_object(const URI& uri) {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }
  START
  LOG << "S3::flush_object: " << uri.c_str() << std::endl;

  // Flush and delete file buffer
  auto buff = (Buffer*)nullptr;
  RETURN_NOT_OK(get_file_buffer(uri, &buff));
  RETURN_NOT_OK(flush_file_buffer(uri, buff, true));

  if (multipart_started_)
    RETURN_NOT_OK(complete_multipart_upload(uri));

  file_buffers_.erase(uri.to_string());
  delete buff;

  LOG << "S3::flush_object" << DONE;

  return Status::Ok();
}

Status S3::complete_multipart_upload(const URI& uri) {
  START
  LOG << "S3::complete_multipart_upload: " << uri.c_str() << std::endl;

  Aws::Http::URI aws_uri = uri.c_str();
  std::string path_c_str = aws_uri.GetPath().c_str();

  // Take a lock protecting the shared multipart data structures
  std::unique_lock<std::mutex> multipart_lck(multipart_upload_mtx_);

  // Do nothing - empty object
  auto multipart_upload_it = multipart_upload_.find(path_c_str);
  if (multipart_upload_it == multipart_upload_.end())
    return Status::Ok();

  // Get the completed upload object
  auto completed_multipart_upload = multipart_upload_it->second;

  // Add all the completed parts (sorted by part number) to the upload object.
  auto completed_parts_it = multipart_upload_completed_parts_.find(path_c_str);
  if (completed_parts_it == multipart_upload_completed_parts_.end()) {
    return LOG_STATUS(Status::S3Error(
        "Unable to find completed parts list for S3 object " +
        uri.to_string()));
  }
  for (auto& tup : completed_parts_it->second) {
    Aws::S3::Model::CompletedPart& part = std::get<1>(tup);
    completed_multipart_upload.AddParts(part);
  }

  auto multipart_upload_request_it = multipart_upload_request_.find(path_c_str);
  auto complete_multipart_upload_request = multipart_upload_request_it->second;
  complete_multipart_upload_request.WithMultipartUpload(
      completed_multipart_upload);
  auto complete_multipart_upload_outcome =
      client_->CompleteMultipartUpload(complete_multipart_upload_request);

  // Release lock while we wait for the file to flush.
  multipart_lck.unlock();

  wait_for_object_to_propagate(
      complete_multipart_upload_request.GetBucket(),
      complete_multipart_upload_request.GetKey());

  multipart_lck.lock();

  multipart_upload_IDs_.erase(path_c_str);
  multipart_upload_part_number_.erase(path_c_str);
  multipart_upload_request_.erase(multipart_upload_request_it);
  multipart_upload_.erase(multipart_upload_it);
  multipart_upload_completed_parts_.erase(path_c_str);

  //  fails when flushing directories or removed files
  if (!complete_multipart_upload_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to flush S3 object ") + uri.c_str() +
        outcome_error_message(complete_multipart_upload_outcome)));
  }

  multipart_started_ = false;
  LOG << "S3::complete_multipart_upload" << DONE;

  return Status::Ok();
}

Status S3::is_empty_bucket(const URI& bucket, bool* is_empty) const {
  if (!is_bucket(bucket))
    return LOG_STATUS(Status::S3Error(
        "Cannot check if bucket is empty; Bucket does not exist"));

  START
  LOG << "S3::is_empty_bucket: " << bucket.c_str() << std::endl;

  Aws::Http::URI aws_uri = bucket.c_str();
  Aws::S3::Model::ListObjectsRequest list_objects_request;
  list_objects_request.SetBucket(aws_uri.GetAuthority());
  list_objects_request.SetPrefix("");
  list_objects_request.SetDelimiter("/");
  auto list_objects_outcome = client_->ListObjects(list_objects_request);

  if (!list_objects_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to list s3 objects in bucket ") + bucket.c_str() +
        outcome_error_message(list_objects_outcome)));
  }

  *is_empty = list_objects_outcome.GetResult().GetContents().empty() &&
              list_objects_outcome.GetResult().GetCommonPrefixes().empty();

  LOG << "S3::is_empty_bucket" << DONE;

  return Status::Ok();
}

bool S3::is_bucket(const URI& bucket) const {
  START
  LOG << "S3::is_bucket" << std::endl;

  if (!bucket.is_s3()) {
    return false;
  }

  Aws::Http::URI aws_uri = bucket.c_str();
  Aws::S3::Model::HeadBucketRequest head_bucket_request;
  head_bucket_request.SetBucket(aws_uri.GetAuthority());
  auto head_bucket_outcome = client_->HeadBucket(head_bucket_request);
  LOG << "S3::is_bucket" << DONE;

  return head_bucket_outcome.IsSuccess();
}

bool S3::is_object(const URI& uri) const {
  START
  LOG << "S3::is_object: " << uri.c_str() << std::endl;

  if (!uri.is_s3())
    return false;

  Aws::Http::URI aws_uri = uri.c_str();
  Aws::S3::Model::HeadObjectRequest head_object_request;
  head_object_request.SetBucket(aws_uri.GetAuthority());
  head_object_request.SetKey(aws_uri.GetPath());
  auto head_object_outcome = client_->HeadObject(head_object_request);
  LOG << "S3::is_object" << DONE;
  return head_object_outcome.IsSuccess();
}

Status S3::is_dir(const URI& uri, bool* exists) const {
  START
  LOG << "S3::is_dir: " << uri.c_str() << std::endl;
  // Potentially add `/` to the end of `uri`
  auto uri_dir = uri.add_trailing_slash();
  std::vector<std::string> paths;
  RETURN_NOT_OK(ls(uri_dir, &paths, "/", 1));
  *exists = (bool)paths.size();
  LOG << "S3::is_dir" << DONE;
  return Status::Ok();
}

Status S3::ls(
    const URI& prefix,
    std::vector<std::string>* paths,
    const std::string& delimiter,
    int max_paths) const {
  auto prefix_str = prefix.to_string();
  if (!prefix.is_s3()) {
    return LOG_STATUS(
        Status::S3Error(std::string("URI is not an S3 URI: " + prefix_str)));
  }

  START
  LOG << "S3::ls: " << prefix.c_str() << std::endl;

  Aws::Http::URI aws_uri = prefix_str.c_str();
  auto aws_prefix = remove_front_slash(aws_uri.GetPath().c_str());
  std::string aws_auth = aws_uri.GetAuthority().c_str();
  Aws::S3::Model::ListObjectsRequest list_objects_request;
  list_objects_request.SetBucket(aws_uri.GetAuthority());
  list_objects_request.SetPrefix(aws_prefix.c_str());
  list_objects_request.SetDelimiter(delimiter.c_str());
  if (max_paths != -1)
    list_objects_request.SetMaxKeys(max_paths);

  bool is_done = false;
  while (!is_done) {
    auto list_objects_outcome = client_->ListObjects(list_objects_request);

    if (!list_objects_outcome.IsSuccess())
      return LOG_STATUS(Status::S3Error(
          std::string("Error while listing with prefix '") + prefix_str +
          "' and delimiter '" + delimiter + "'" +
          outcome_error_message(list_objects_outcome)));

    for (const auto& object : list_objects_outcome.GetResult().GetContents()) {
      std::string file(object.GetKey().c_str());
      paths->push_back("s3://" + aws_auth + add_front_slash(file));
    }

    for (const auto& object :
         list_objects_outcome.GetResult().GetCommonPrefixes()) {
      std::string file(object.GetPrefix().c_str());
      paths->push_back("s3://" + aws_auth + add_front_slash(file));
    }

    is_done = !list_objects_outcome.GetResult().GetIsTruncated();
    if (!is_done)
      list_objects_request.SetMarker(
          list_objects_outcome.GetResult().GetNextMarker());
  }

  LOG << "S3::ls" << DONE;

  return Status::Ok();
}

Status S3::move_object(const URI& old_uri, const URI& new_uri) {
  START
  LOG << "S3::move_object: " << old_uri.c_str() << std::endl;
  RETURN_NOT_OK(copy_object(old_uri, new_uri));
  RETURN_NOT_OK(remove_object(old_uri));
  LOG << "S3::move_object" << DONE;
  return Status::Ok();
}

Status S3::move_dir(const URI& old_uri, const URI& new_uri) {
  START
  LOG << "S3::move_dir: " << old_uri.c_str() << std::endl;
  std::vector<std::string> paths;
  RETURN_NOT_OK(ls(old_uri, &paths, ""));
  for (const auto& path : paths) {
    auto suffix = path.substr(old_uri.to_string().size());
    auto new_path = new_uri.join_path(suffix);
    RETURN_NOT_OK(move_object(URI(path), URI(new_path)));
  }
  LOG << "S3::move_object" << DONE;
  return Status::Ok();
}

Status S3::object_size(const URI& uri, uint64_t* nbytes) const {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  START  
  LOG << "S3::object_size: " << uri.c_str() << std::endl;

  Aws::Http::URI aws_uri = uri.to_string().c_str();
  std::string aws_path = remove_front_slash(aws_uri.GetPath().c_str());
  Aws::S3::Model::ListObjectsRequest list_objects_request;
  list_objects_request.SetBucket(aws_uri.GetAuthority());
  list_objects_request.SetPrefix(aws_path.c_str());
  auto list_objects_outcome = client_->ListObjects(list_objects_request);

  if (!list_objects_outcome.IsSuccess())
    return LOG_STATUS(Status::S3Error(
        "Cannot retrieve S3 object size; Error while listing file " +
        uri.to_string() + outcome_error_message(list_objects_outcome)));
  if (list_objects_outcome.GetResult().GetContents().empty())
    return LOG_STATUS(Status::S3Error(
        std::string("Cannot retrieve S3 object size; Not a file ") +
        uri.to_string()));
  *nbytes = static_cast<uint64_t>(
      list_objects_outcome.GetResult().GetContents()[0].GetSize());

  LOG << "S3::object_size" << DONE;

  return Status::Ok();
}

Status S3::read(
    const URI& uri, off_t offset, void* buffer, uint64_t length) const {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  START
  LOG << "S3::read: " << uri.c_str() << std::endl;

  Aws::Http::URI aws_uri = uri.c_str();
  Aws::S3::Model::GetObjectRequest get_object_request;
  get_object_request.WithBucket(aws_uri.GetAuthority())
      .WithKey(aws_uri.GetPath());
  get_object_request.SetRange(("bytes=" + std::to_string(offset) + "-" +
                               std::to_string(offset + length - 1))
                                  .c_str());
  get_object_request.SetResponseStreamFactory([buffer, length]() {
    auto streamBuf = new boost::interprocess::bufferbuf((char*)buffer, length);
    return Aws::New<Aws::IOStream>(
        constants::s3_allocation_tag.c_str(), streamBuf);
  });

  auto get_object_outcome = client_->GetObject(get_object_request);
  if (!get_object_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to read S3 object ") + uri.c_str() +
        outcome_error_message(get_object_outcome)));
  }
  if ((uint64_t)get_object_outcome.GetResult().GetContentLength() != length) {
    return LOG_STATUS(Status::S3Error(
        std::string("Read operation returned different size of bytes.")));
  }

  LOG << "S3::read" << DONE;
  LOG << "S3::read bytes " << length << "\n";

  return Status::Ok();
}

Status S3::remove_object(const URI& uri) const {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  START
  LOG << "S3::remove_object: " << uri.c_str() << std::endl;

  Aws::Http::URI aws_uri = uri.to_string().c_str();
  Aws::S3::Model::DeleteObjectRequest delete_object_request;
  delete_object_request.SetBucket(aws_uri.GetAuthority());
  delete_object_request.SetKey(aws_uri.GetPath());

  auto delete_object_outcome = client_->DeleteObject(delete_object_request);
  if (!delete_object_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to delete S3 object '") + uri.c_str() +
        outcome_error_message(delete_object_outcome)));
  }

  wait_for_object_to_be_deleted(
      delete_object_request.GetBucket(), delete_object_request.GetKey());

  LOG << "S3::remove_object" << DONE;

  return Status::Ok();
}

Status S3::remove_dir(const URI& uri) const {
  START
  LOG << "S3::remove_dir: " << uri.c_str() << std::endl;
  std::vector<std::string> paths;
  auto uri_dir = uri.add_trailing_slash();
  RETURN_NOT_OK(ls(uri_dir, &paths, ""));
  for (const auto& p : paths)
    RETURN_NOT_OK(remove_object(URI(p)));
  LOG << "S3::remove_dir" << DONE;
  return Status::Ok();
}

Status S3::touch(const URI& uri) const {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(std::string(
        "Cannot create file; URI is not an S3 URI: " + uri.to_string())));
  }

  START
  LOG << "S3::touch: " << uri.c_str() << std::endl;

  Aws::Http::URI aws_uri = uri.c_str();
  Aws::S3::Model::PutObjectRequest put_object_request;
  put_object_request.WithKey(aws_uri.GetPath())
      .WithBucket(aws_uri.GetAuthority());

  auto request_stream =
      Aws::MakeShared<Aws::StringStream>(constants::s3_allocation_tag.c_str());
  put_object_request.SetBody(request_stream);

  auto put_object_outcome = client_->PutObject(put_object_request);
  if (!put_object_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Cannot touch object '") + uri.c_str() +
        outcome_error_message(put_object_outcome)));
  }

  wait_for_object_to_propagate(
      put_object_request.GetBucket(), put_object_request.GetKey());

  LOG << "S3::touch" << DONE;

  return Status::Ok();
}

Status S3::write(const URI& uri, const void* buffer, uint64_t length) {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  START
  LOG << "S3::write: " << uri.c_str() << std::endl;

  // This write is never considered the last part of an object. The last part is
  // only uploaded with flush_object().
  const bool is_last_part = false;

  // Get file buffer
  auto buff = (Buffer*)nullptr;
  RETURN_NOT_OK(get_file_buffer(uri, &buff));
  
  // Fill file buffer
  uint64_t nbytes_filled;
  RETURN_NOT_OK(fill_file_buffer(buff, buffer, length, &nbytes_filled));

  // Flush file buffer
  if (buff->size() == file_buffer_size_)
    RETURN_NOT_OK(flush_file_buffer(uri, buff, is_last_part));

  // Write chunks
  uint64_t new_length = length - nbytes_filled;
  uint64_t offset = nbytes_filled;
  while (new_length > 0) {
    if (new_length >= file_buffer_size_) {
      RETURN_NOT_OK(write_multipart(
          uri, (char*)buffer + offset, file_buffer_size_, is_last_part));
      offset += file_buffer_size_;
      new_length -= file_buffer_size_;
    } else {
      RETURN_NOT_OK(fill_file_buffer(
          buff, (char*)buffer + offset, new_length, &nbytes_filled));
      offset += nbytes_filled;
      new_length -= nbytes_filled;
    }
  }
  assert(offset == length);

  LOG << "S3::write" << DONE;

  return Status::Ok();
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

Status S3::copy_object(const URI& old_uri, const URI& new_uri) {
  START
  LOG << "S3::copy_object: " << old_uri.c_str() << std::endl;

  Aws::Http::URI src_uri = old_uri.c_str();
  Aws::Http::URI dst_uri = new_uri.c_str();
  Aws::S3::Model::CopyObjectRequest copy_object_request;
  copy_object_request.SetCopySource(
      join_authority_and_path(
          src_uri.GetAuthority().c_str(), src_uri.GetPath().c_str())
          .c_str());
  copy_object_request.SetBucket(dst_uri.GetAuthority());
  copy_object_request.SetKey(dst_uri.GetPath());

  auto copy_object_outcome = client_->CopyObject(copy_object_request);
  if (!copy_object_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to copy S3 object ") + old_uri.c_str() + " to " +
        new_uri.c_str() + outcome_error_message(copy_object_outcome)));
  }

  wait_for_object_to_propagate(
      copy_object_request.GetBucket(), copy_object_request.GetKey());

  LOG << "S3::copy_object" << DONE;

  return Status::Ok();
}

Status S3::fill_file_buffer(
    Buffer* buff,
    const void* buffer,
    uint64_t length,
    uint64_t* nbytes_filled) {
  STATS_FUNC_IN(vfs_s3_fill_file_buffer);

  START
  LOG << "S3::fill_file_buffer" << std::endl;

  *nbytes_filled = std::min(file_buffer_size_ - buff->size(), length);
  if (*nbytes_filled > 0)
    RETURN_NOT_OK(buff->write(buffer, *nbytes_filled));

  LOG << "S3::fill_file_buffer" << DONE;
  return Status::Ok();

  STATS_FUNC_OUT(vfs_s3_fill_file_buffer);
}

std::string S3::add_front_slash(const std::string& path) const {
  return (path.front() != '/') ? (std::string("/") + path) : path;
}

std::string S3::remove_front_slash(const std::string& path) const {
  if (path.front() == '/')
    return path.substr(1, path.length());
  return path;
}

Status S3::flush_file_buffer(const URI& uri, Buffer* buff, bool last_part) {
  START
  LOG << "S3::flush_file_buffer: " << uri.c_str() << std::endl;
  if (buff->size() > 0) {
    RETURN_NOT_OK(write_multipart(uri, buff->data(), buff->size(), last_part));
    buff->reset_size();
  }
  LOG << "S3::flush_file_buffer" << DONE;
  return Status::Ok();
}

Status S3::get_file_buffer(const URI& uri, Buffer** buff) {
  START
  LOG << "S3::get_file_buffer: " << uri.c_str() << std::endl;
  std::unique_lock<std::mutex> lck(multipart_upload_mtx_);

  auto uri_str = uri.to_string();
  auto it = file_buffers_.find(uri_str);
  if (it == file_buffers_.end()) {
    auto new_buff = new Buffer();
    file_buffers_[uri_str] = new_buff;
    *buff = new_buff;
  } else {
    *buff = it->second;
  }
  LOG << "S3::get_file_buffer" << DONE;
  return Status::Ok();
}

Status S3::initiate_multipart_request(Aws::Http::URI aws_uri) {
  START
  LOG << "S3::initiate_multipart_request: " << aws_uri.GetPath().c_str() << std::endl;
  auto& path = aws_uri.GetPath();
  std::string path_c_str = path.c_str();
  Aws::S3::Model::CreateMultipartUploadRequest multipart_upload_request;
  multipart_upload_request.SetBucket(aws_uri.GetAuthority());
  multipart_upload_request.SetKey(path);
  multipart_upload_request.SetContentType("application/octet-stream");

  auto multipart_upload_outcome =
      client_->CreateMultipartUpload(multipart_upload_request);
  if (!multipart_upload_outcome.IsSuccess()) {
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to create multipart request for object '") +
        path_c_str + outcome_error_message(multipart_upload_outcome)));
  }

  multipart_upload_IDs_[path_c_str] =
      multipart_upload_outcome.GetResult().GetUploadId();
  multipart_upload_part_number_[path_c_str] = 1;

  Aws::S3::Model::CompleteMultipartUploadRequest
      complete_multipart_upload_request;
  complete_multipart_upload_request.SetBucket(aws_uri.GetAuthority());
  complete_multipart_upload_request.SetKey(path);
  complete_multipart_upload_request.SetUploadId(
      multipart_upload_IDs_[path_c_str]);

  Aws::S3::Model::CompletedMultipartUpload completed_multipart_upload;
  multipart_upload_[path_c_str] = completed_multipart_upload;
  multipart_upload_request_[path_c_str] = complete_multipart_upload_request;
  multipart_upload_completed_parts_[path_c_str] =
      std::map<int, Aws::S3::Model::CompletedPart>();

  LOG << "S3::initiate_multipart_request" << DONE;
  return Status::Ok();
}

std::string S3::join_authority_and_path(
    const std::string& authority, const std::string& path) const {
  bool path_has_slash = !path.empty() && path.front() == '/';
  bool authority_has_slash = !authority.empty() && authority.back() == '/';
  bool need_slash = !(path_has_slash || authority_has_slash);
  return authority + (need_slash ? "/" : "") + path;
}

bool S3::wait_for_object_to_propagate(
    const Aws::String& bucketName, const Aws::String& objectKey) const {
  START
  LOG << "S3::wait_for_object_to_propagate: " << objectKey.c_str() << std::endl;
  unsigned attempts_cnt = 0;
  while (attempts_cnt++ < constants::s3_max_attempts) {
    Aws::S3::Model::HeadObjectRequest head_object_request;
    head_object_request.SetBucket(bucketName);
    head_object_request.SetKey(objectKey);
    auto headObjectOutcome = client_->HeadObject(head_object_request);
    if (headObjectOutcome.IsSuccess()) {
      LOG << "S3::wait_for_object_to_propagate" << DONE;
      return true;
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::s3_attempt_sleep_ms));
  }
  
  LOG << "S3::wait_for_object_to_propagate" << DONE;
  return false;
}

bool S3::wait_for_object_to_be_deleted(
    const Aws::String& bucketName, const Aws::String& objectKey) const {
  START
  LOG << "S3::wait_for_object_to_be_deleted: " << objectKey.c_str() << std::endl;
  unsigned attempts_cnt = 0;
  while (attempts_cnt++ < constants::s3_max_attempts) {
    Aws::S3::Model::HeadObjectRequest head_object_request;
    head_object_request.SetBucket(bucketName);
    head_object_request.SetKey(objectKey);
    auto head_object_outcome = client_->HeadObject(head_object_request);
    if (!head_object_outcome.IsSuccess()) {
      LOG << "S3::wait_for_object_to_be_deleted" << DONE;
      return true;
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::s3_attempt_sleep_ms));
  }

  LOG << "S3::wait_for_object_to_be_deleted" << DONE;
  return false;
}

bool S3::wait_for_bucket_to_be_created(const URI& bucket_uri) const {
  START
  LOG << "S3::wait_for_bucket_to_be_created: " << bucket_uri.c_str() << std::endl;
  unsigned attempts_cnt = 0;
  while (attempts_cnt++ < constants::s3_max_attempts) {
    if (is_bucket(bucket_uri)) {
      LOG << "S3::wait_for_bucket_to_be_created" << DONE;
      return true;
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(constants::s3_attempt_sleep_ms));
  }
  LOG << "S3::wait_for_bucket_to_be_created" << DONE;
  return false;
}

Status S3::write_multipart(
    const URI& uri, const void* buffer, uint64_t length, bool last_part) {
  STATS_FUNC_IN(vfs_s3_write_multipart);
  START
  LOG << "S3::write_multipart: " << uri.c_str() << std::endl;
  // Ensure that each thread is responsible for exactly multipart_part_size_
  // bytes (except if this is the last write_multipart, in which case the final
  // thread should write less), and cap the number of parallel operations at the
  // configured max number.
  uint64_t num_ops = last_part ? utils::ceil(length, multipart_part_size_) :
                                 (length / multipart_part_size_);
  num_ops = std::min(std::max(num_ops, uint64_t(1)), max_parallel_ops_);

  if (!last_part && length % multipart_part_size_ != 0) {
    LOG << "S3::write_multipart" << DONE;
    return LOG_STATUS(
        Status::S3Error("Length not evenly divisible by part length"));
  }

 if (num_ops == 1 && !multipart_started_) {
    LOG << "S3::write_multipart" << DONE;
    return put(uri, buffer, length);
 }

 else {
  multipart_started_ = true;
  Aws::Http::URI aws_uri = uri.c_str();
  auto& path = aws_uri.GetPath();
  std::string path_c_str = path.c_str();

  // Take a lock protecting the shared multipart data structures
  std::unique_lock<std::mutex> multipart_lck(multipart_upload_mtx_);

  if (multipart_upload_IDs_.find(path_c_str) == multipart_upload_IDs_.end()) {
    // Delete file if it exists (overwrite) and initiate multipart request
    if (is_object(uri))
      RETURN_NOT_OK(remove_object(uri));
    Status st = initiate_multipart_request(aws_uri);
    if (!st.ok()) {
      LOG << "S3::write_multipart" << DONE;
      return st;
    }
  }

  // Get the upload ID
  auto upload_id = multipart_upload_IDs_[path_c_str];

  // Assign the part number(s), and make the write request.
  if (num_ops == 1) {
    int part_num = multipart_upload_part_number_[path_c_str]++;
    auto buff = new Buffer();
    buff->write(buffer, length);
    parts_[part_num] = buff;

    // Unlock, as make_upload_part_req will reaquire as necessary.
    multipart_lck.unlock();
    LOG << "S3::write_multipart" << DONE;
    Status status;
    do {
      std::chrono::system_clock::time_point time_interval = 
        std::chrono::system_clock::now() + std::chrono::milliseconds(600);
      std::future<Status> part_fut = vfs_thread_pool_->enqueue(
          [this, &uri, length, &upload_id, part_num]() {
            return make_upload_part_req(
                uri, length, upload_id, part_num);
          });
      status = vfs_thread_pool_->get_status(part_fut, time_interval);
    } while (status.code() == StatusCode::Timeout);
    return status;
  } else {
    STATS_COUNTER_ADD(vfs_s3_write_num_parallelized, 1);

    std::vector<std::future<Status>> results;
    uint64_t bytes_per_op = multipart_part_size_;
    int part_num_base = multipart_upload_part_number_[path_c_str];
    // for (uint64_t i = 0; i < num_ops; i++) {
    //   uint64_t begin = i * bytes_per_op,
    //            end = std::min((i + 1) * bytes_per_op - 1, length - 1);
    //   uint64_t thread_nbytes = end - begin + 1;
    //   auto thread_buffer = reinterpret_cast<const char*>(buffer) + begin;
    //   int part_num = static_cast<int>(part_num_base + i);
    //   results.push_back(vfs_thread_pool_->enqueue(
    //       [this, &uri, thread_buffer, thread_nbytes, &upload_id, part_num]() {
    //         return make_upload_part_req(
    //             uri, thread_buffer, thread_nbytes, upload_id, part_num);
    //       }));
    // }
    // multipart_upload_part_number_[path_c_str] += num_ops;

    // // Unlock, so the threads can take the lock as necessary.
    // multipart_lck.unlock();

    std::vector<uint64_t> threadbytes;
    std::vector<int> partnums;
    for (uint64_t i = 0; i < num_ops; i++) {
      uint64_t begin = i * bytes_per_op,
               end = std::min((i + 1) * bytes_per_op - 1, length - 1);
      uint64_t thread_nbytes = end - begin + 1;
      int part_num = static_cast<int>(part_num_base + i);
      threadbytes.push_back(thread_nbytes);
      partnums.push_back(part_num);
      auto thread_buffer = reinterpret_cast<const char*>(buffer) + begin;
      auto buff = new Buffer();
      buff->write(thread_buffer, thread_nbytes);
      parts_[part_num] = buff;
    }

    for (uint64_t i = 0; i < num_ops; i++) {
        uint64_t thread_nbytes = threadbytes[i];
        uint64_t part_num = partnums[i];
        results.push_back(vfs_thread_pool_->enqueue(
          [this, &uri, thread_nbytes, &upload_id, part_num]() {
            return make_upload_part_req(
                uri, thread_nbytes, upload_id, part_num);
          }));
    }
    multipart_upload_part_number_[path_c_str] += num_ops;

    // Unlock, so the threads can take the lock as necessary.
    multipart_lck.unlock();

    std::chrono::system_clock::time_point wait_period = 
      std::chrono::system_clock::now() + std::chrono::milliseconds(600);
    auto statuses = vfs_thread_pool_->wait_for_time_status(results, wait_period);

    std::vector<Status> all_status;
    std::vector<std::future<Status>> retry;
    std::vector<int> retry_parts;
    
    for(uint64_t i = 0; i < partnums.size(); ++i) {
      auto status = statuses[i];
      std::cout << "Part " << partnums[i];
      if (status.code() == StatusCode::Timeout) {
        std::cout << " has timed out\n";
        retry_parts.push_back(i);
      }
      else {
        std::cout << " completed ok\n";
        all_status.push_back(status);
      }
    }

    do {
      
      retry.clear();

      for(uint64_t i = 0; i < retry_parts.size(); i++) {
        auto part = retry_parts[i];
        uint64_t part_num = partnums[part];

          uint64_t thread_nbytes = threadbytes[part];
          
          std::cout << part_num << " took too long, retrying\n";
          retry.push_back(vfs_thread_pool_->enqueue(
            [this, &uri, thread_nbytes, &upload_id, part_num]() {
              return make_upload_part_req(
                  uri, thread_nbytes, upload_id, part_num);
            }));
      }  

      std::chrono::system_clock::time_point wait_period = 
        std::chrono::system_clock::now() + std::chrono::milliseconds(400);
      auto retries = vfs_thread_pool_->wait_for_time_status(retry, wait_period);
      
      auto idx = retry_parts.begin();
      for(uint64_t i = 0; i < retries.size(); i++) {
        auto status = retries[i];
        if (status.code() != StatusCode::Timeout) {
          idx = retry_parts.erase(idx);
          all_status.push_back(status);
        }
        else {
          ++idx;
        }
      }

    } while (retry_parts.size() > 0);

    bool all_ok = true;
    for (auto status : all_status) {
      std::cout << status.to_string() << std::endl;
      if (!status.ok()){
        all_ok = false;
      }
    }

    // bool all_ok = vfs_thread_pool_->wait_all(results);
    LOG << "S3::write_multipart" << DONE;
    return all_ok ?
               Status::Ok() :
               LOG_STATUS(Status::S3Error("S3 parallel write_multipart error"));
  }
 }

  STATS_FUNC_OUT(vfs_s3_write_multipart);
}

Status S3::make_upload_part_req(
    const URI& uri,
    uint64_t length,
    const Aws::String& upload_id,
    int upload_part_num) {
  START
  LOG << "S3::make_upload_part_req #" << upload_part_num << " to " << uri.c_str() << std::endl;
  Aws::Http::URI aws_uri = uri.c_str();
  auto& path = aws_uri.GetPath();
  std::string path_c_str = path.c_str();
  
  auto stream = std::shared_ptr<Aws::IOStream>(
    new boost::interprocess::bufferstream(
      (char*)parts_[upload_part_num]->data(), length));

  Aws::S3::Model::UploadPartRequest upload_part_request;
  upload_part_request.SetBucket(aws_uri.GetAuthority());
  upload_part_request.SetKey(path);
  upload_part_request.SetPartNumber(upload_part_num);
  upload_part_request.SetUploadId(upload_id);
  upload_part_request.SetBody(stream);
  upload_part_request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(
      Aws::Utils::HashingUtils::CalculateMD5(*stream)));
  upload_part_request.SetContentLength(length);


  auto upload_part_outcome = client_->UploadPart(upload_part_request);

  if (!upload_part_outcome.IsSuccess()) {
    LOG << "S3::make_upload_part_req #" << upload_part_num << DONE;
    return LOG_STATUS(Status::S3Error(
        std::string("Failed to upload part of S3 object '") //+ uri.c_str() +
        + outcome_error_message(upload_part_outcome)));
  }

  Aws::S3::Model::CompletedPart completed_part;
  completed_part.SetETag(upload_part_outcome.GetResult().GetETag());
  completed_part.SetPartNumber(upload_part_num);

  {
    std::unique_lock<std::mutex> lck(multipart_upload_mtx_);
    if ( multipart_upload_completed_parts_[path_c_str].count(upload_part_num) == 0) {
      multipart_upload_completed_parts_[path_c_str][upload_part_num] =
        completed_part;
    }
  }

  STATS_COUNTER_ADD(vfs_s3_num_parts_written, 1);

  LOG << "S3::make_upload_part_req #" << upload_part_num << DONE;
  return Status::Ok();
}


Status S3::make_upload_part_req_timeout(
    const URI& uri,
    // const void* buffer,
    uint64_t length,
    const Aws::String& upload_id,
    int upload_part_num, 
    std::chrono::system_clock::time_point time_interval) {
  START
  LOG << "S3::make_upload_part_req #" << upload_part_num << " to " << uri.c_str() << std::endl;
  Aws::Http::URI aws_uri = uri.c_str();
  auto& path = aws_uri.GetPath();
  std::string path_c_str = path.c_str();

  auto stream = std::shared_ptr<Aws::IOStream>(
    new boost::interprocess::bufferstream(
      (char*)parts_[upload_part_num]->data(), length));

  Aws::S3::Model::UploadPartRequest upload_part_request;
  upload_part_request.SetBucket(aws_uri.GetAuthority());
  upload_part_request.SetKey(path);
  upload_part_request.SetPartNumber(upload_part_num);
  upload_part_request.SetUploadId(upload_id);
  upload_part_request.SetBody(stream);
  upload_part_request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(
      Aws::Utils::HashingUtils::CalculateMD5(*stream)));
  upload_part_request.SetContentLength(length);

  auto upload_part_outcome_callable =
      client_->UploadPartCallable(upload_part_request);

  if ( upload_part_outcome_callable.wait_until(time_interval) != std::future_status::ready ) {
    return Status::TimeoutError("Future timed out");
  }
  else {
    auto upload_part_outcome = upload_part_outcome_callable.get();

    if (!upload_part_outcome.IsSuccess()) {
      return Status::TimeoutError("Future timed out");
    }

    Aws::S3::Model::CompletedPart completed_part;
    completed_part.SetETag(upload_part_outcome.GetResult().GetETag());
    completed_part.SetPartNumber(upload_part_num);

    {
      std::unique_lock<std::mutex> lck(multipart_upload_mtx_);
      multipart_upload_completed_parts_[path_c_str][upload_part_num] =
          completed_part;
    }

    STATS_COUNTER_ADD(vfs_s3_num_parts_written, 1);

    LOG << "S3::make_upload_part_req #" << upload_part_num << DONE;
    return Status::Ok();
  }
}

Status S3::put(const URI& uri, const void* buffer, uint64_t length) {
  if (!uri.is_s3()) {
    return LOG_STATUS(Status::S3Error(
        std::string("URI is not an S3 URI: " + uri.to_string())));
  }

  START
  LOG << "S3::put: " << uri.c_str() << std::endl;

    Aws::Http::URI aws_uri = uri.c_str();
    auto& path = aws_uri.GetPath();

    Aws::S3::Model::PutObjectRequest object_request;
    object_request.SetBucket(aws_uri.GetAuthority());
    object_request.SetKey(path);
    
    auto input_data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream");
    input_data->write((char*)buffer, length);

    object_request.SetBody(input_data);

    auto put_object_outcome = client_->PutObject(object_request);

    // wait_for_object_to_propagate(
    //   object_request.GetBucket(),
    //   object_request.GetKey());

    if ( !put_object_outcome.IsSuccess() ) {
        std::string err = (put_object_outcome.GetError().GetMessage()).c_str();
        LOG << "S3::put" << DONE;
        return LOG_STATUS(Status::S3Error(
          std::string("Cannot write object ") + uri.c_str() + err));
    }

  LOG << "S3::put" << DONE;
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif
