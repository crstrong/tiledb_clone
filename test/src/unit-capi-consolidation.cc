/**
 * @file   unit-capi-consolidation.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB Inc.
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
 * Tests the C API consolidation.
 */

#include "catch.hpp"
#include "tiledb/sm/c_api/tiledb.h"

#include <cstring>

/** Tests for C API consolidation. */
struct ConsolidationFx {
  // Constants
  const char* DENSE_ARRAY_NAME = "test_consolidate_dense";
  const char* SPARSE_ARRAY_NAME = "test_consolidate_sparse";

  // TileDB context
  tiledb_ctx_t* ctx_;

  // Constructors/destructors
  ConsolidationFx();
  ~ConsolidationFx();

  // Functions
  void create_dense_array();
  void create_sparse_array();
  void write_dense_full();
  void write_dense_subarray();
  void write_dense_unordered();
  void write_sparse_full();
  void write_sparse_unordered();
  void read_dense_full_subarray_unordered();
  void read_dense_subarray_full_unordered();
  void read_dense_subarray_unordered_full();
  void read_sparse_full_unordered();
  void read_sparse_unordered_full();
  void consolidate_dense();
  void consolidate_sparse();
  void remove_dense_array();
  void remove_sparse_array();
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);
};

ConsolidationFx::ConsolidationFx() {
  ctx_ = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx_) == TILEDB_OK);
}

ConsolidationFx::~ConsolidationFx() {
  tiledb_ctx_free(&ctx_);
}

void ConsolidationFx::create_dense_array() {
  // Create dimensions
  uint64_t dim_domain[] = {1, 4, 1, 4};
  uint64_t tile_extents[] = {2, 2};
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  CHECK(rc == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx_, "a1", TILEDB_INT32, &a1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a1, TILEDB_BLOSC_LZ, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a1, 1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a2;
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_alloc(ctx_, "a2", TILEDB_CHAR, &a2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a2, TILEDB_GZIP, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a2, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a3;
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_alloc(ctx_, "a3", TILEDB_FLOAT32, &a3);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a3, TILEDB_ZSTD, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a3, 2);
  CHECK(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a3);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, DENSE_ARRAY_NAME, array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_attribute_free(&a2);
  tiledb_attribute_free(&a3);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void ConsolidationFx::create_sparse_array() {
  // Create dimensions
  uint64_t dim_domain[] = {1, 4, 1, 4};
  uint64_t tile_extents[] = {2, 2};
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  CHECK(rc == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx_, "a1", TILEDB_INT32, &a1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a1, TILEDB_BLOSC_LZ, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a1, 1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a2;
  rc = tiledb_attribute_alloc(ctx_, "a2", TILEDB_CHAR, &a2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a2, TILEDB_GZIP, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a2, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a3;
  rc = tiledb_attribute_alloc(ctx_, "a3", TILEDB_FLOAT32, &a3);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a3, TILEDB_ZSTD, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a3, 2);
  CHECK(rc == TILEDB_OK);

  // Create array schmea
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, 2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a3);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, SPARSE_ARRAY_NAME, array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_attribute_free(&a2);
  tiledb_attribute_free(&a3);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void ConsolidationFx::write_dense_full() {
  // Set attributes
  const char* attributes[] = {"a1", "a2", "a3"};

  // Prepare cell buffers
  // clang-format off
  int buffer_a1[] = {
      0,  1,  2,  3, 4,  5,  6,  7,
      8,  9,  10, 11, 12, 13, 14, 15
  };
  uint64_t buffer_a2[] = {
      0,  1,  3,  6, 10, 11, 13, 16,
      20, 21, 23, 26, 30, 31, 33, 36
  };
  char buffer_var_a2[] =
      "abbcccdddd"
      "effggghhhh"
      "ijjkkkllll"
      "mnnooopppp";
  float buffer_a3[] = {
      0.1f,  0.2f,  1.1f,  1.2f,  2.1f,  2.2f,  3.1f,  3.2f,
      4.1f,  4.2f,  5.1f,  5.2f,  6.1f,  6.2f,  7.1f,  7.2f,
      8.1f,  8.2f,  9.1f,  9.2f,  10.1f, 10.2f, 11.1f, 11.2f,
      12.1f, 12.2f, 13.1f, 13.2f, 14.1f, 14.2f, 15.1f, 15.2f,
  };
  void* buffers[] = { buffer_a1, buffer_a2, buffer_var_a2, buffer_a3 };
  uint64_t buffer_sizes[] =
  {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2)-1,  // No need to store the last '\0' character
      sizeof(buffer_a3)
  };
  // clang-format on

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void ConsolidationFx::write_dense_subarray() {
  // Prepare cell buffers
  int buffer_a1[] = {112, 113, 114, 115};
  uint64_t buffer_a2[] = {0, 1, 3, 6};
  char buffer_var_a2[] = "MNNOOOPPPP";
  float buffer_a3[] = {
      112.1f, 112.2f, 113.1f, 113.2f, 114.1f, 114.2f, 115.1f, 115.2f};
  void* buffers[] = {buffer_a1, buffer_a2, buffer_var_a2, buffer_a3};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3"};
  uint64_t subarray[] = {3, 4, 3, 4};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void ConsolidationFx::write_dense_unordered() {
  // Prepare buffers
  int buffer_a1[] = {211, 213, 212, 208};
  uint64_t buffer_a2[] = {0, 4, 6, 7};
  char buffer_var_a2[] = "wwwwyyxu";
  float buffer_a3[] = {
      211.1f, 211.2f, 213.1f, 213.2f, 212.1f, 212.2f, 208.1f, 208.2f};
  uint64_t buffer_coords[] = {4, 2, 3, 4, 3, 3, 3, 1};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void ConsolidationFx::write_sparse_full() {
  // Prepare cell buffers
  int buffer_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t buffer_a2[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char buffer_var_a2[] = "abbcccddddeffggghhhh";
  float buffer_a3[] = {0.1f,
                       0.2f,
                       1.1f,
                       1.2f,
                       2.1f,
                       2.2f,
                       3.1f,
                       3.2f,
                       4.1f,
                       4.2f,
                       5.1f,
                       5.2f,
                       6.1f,
                       6.2f,
                       7.1f,
                       7.2f};
  uint64_t buffer_coords[] = {1, 1, 1, 2, 1, 4, 2, 3, 3, 1, 4, 2, 3, 3, 3, 4};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void ConsolidationFx::write_sparse_unordered() {
  // Prepare cell buffers
  int buffer_a1[] = {107, 104, 106, 105};
  uint64_t buffer_a2[] = {0, 3, 4, 5};
  char buffer_var_a2[] = "yyyuwvvvv";
  float buffer_a3[] = {
      107.1f, 107.2f, 104.1f, 104.2f, 106.1f, 106.2f, 105.1f, 105.2f};
  uint64_t buffer_coords[] = {3, 4, 3, 2, 3, 3, 4, 1};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void ConsolidationFx::read_dense_full_subarray_unordered() {
  // Correct buffers
  int c_buffer_a1[] = {
      0, 1, 2, 3, 4, 5, 6, 7, 208, 9, 10, 211, 212, 213, 114, 115};
  uint64_t c_buffer_a2_off[] = {
      0, 1, 3, 6, 10, 11, 13, 16, 20, 21, 23, 26, 30, 31, 33, 36};
  char c_buffer_a2_val[] =
      "abbcccdddd"
      "effggghhhh"
      "ujjkkkwwww"
      "xyyOOOPPPP";
  float c_buffer_a3[] = {
      0.1f,   0.2f,   1.1f,   1.2f,   2.1f,   2.2f,   3.1f,   3.2f,
      4.1f,   4.2f,   5.1f,   5.2f,   6.1f,   6.2f,   7.1f,   7.2f,
      208.1f, 208.2f, 9.1f,   9.2f,   10.1f,  10.2f,  211.1f, 211.2f,
      212.1f, 212.2f, 213.1f, 213.2f, 114.1f, 114.2f, 115.1f, 115.2f,
  };

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a3", subarray, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) == buffer_a1_size);
  CHECK(sizeof(c_buffer_a2_off) == buffer_a2_off_size);
  CHECK(sizeof(c_buffer_a2_val) - 1 == buffer_a2_val_size);
  CHECK(sizeof(c_buffer_a3) == buffer_a3_size);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
}

void ConsolidationFx::read_dense_subarray_full_unordered() {
  // Correct buffers
  int c_buffer_a1[] = {
      0, 1, 2, 3, 4, 5, 6, 7, 208, 9, 10, 211, 212, 213, 14, 15};
  uint64_t c_buffer_a2_off[] = {
      0, 1, 3, 6, 10, 11, 13, 16, 20, 21, 23, 26, 30, 31, 33, 36};
  char c_buffer_a2_val[] =
      "abbcccdddd"
      "effggghhhh"
      "ujjkkkwwww"
      "xyyooopppp";
  float c_buffer_a3[] = {
      0.1f,   0.2f,   1.1f,   1.2f,   2.1f,  2.2f,  3.1f,   3.2f,
      4.1f,   4.2f,   5.1f,   5.2f,   6.1f,  6.2f,  7.1f,   7.2f,
      208.1f, 208.2f, 9.1f,   9.2f,   10.1f, 10.2f, 211.1f, 211.2f,
      212.1f, 212.2f, 213.1f, 213.2f, 14.1f, 14.2f, 15.1f,  15.2f,
  };

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a3", subarray, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
}

void ConsolidationFx::read_dense_subarray_unordered_full() {
  // Correct buffers
  int c_buffer_a1[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  uint64_t c_buffer_a2_off[] = {
      0, 1, 3, 6, 10, 11, 13, 16, 20, 21, 23, 26, 30, 31, 33, 36};
  char c_buffer_a2_val[] =
      "abbcccdddd"
      "effggghhhh"
      "ijjkkkllll"
      "mnnooopppp";
  float c_buffer_a3[] = {
      0.1f,  0.2f,  1.1f,  1.2f,  2.1f,  2.2f,  3.1f,  3.2f,
      4.1f,  4.2f,  5.1f,  5.2f,  6.1f,  6.2f,  7.1f,  7.2f,
      8.1f,  8.2f,  9.1f,  9.2f,  10.1f, 10.2f, 11.1f, 11.2f,
      12.1f, 12.2f, 13.1f, 13.2f, 14.1f, 14.2f, 15.1f, 15.2f,
  };

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, DENSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a3", subarray, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
}

void ConsolidationFx::read_sparse_full_unordered() {
  // Correct buffers
  int c_buffer_a1[] = {0, 1, 2, 3, 4, 104, 105, 5, 106, 107};
  uint64_t c_buffer_a2_off[] = {0, 1, 3, 6, 10, 11, 12, 16, 18, 19};
  char c_buffer_a2_val[] = "abbcccddddeuvvvvffwyyy";
  float c_buffer_a3[] = {0.1f, 0.2f, 1.1f,   1.2f,   2.1f,   2.2f,   3.1f,
                         3.2f, 4.1f, 4.2f,   104.1f, 104.2f, 105.1f, 105.2f,
                         5.1f, 5.2f, 106.1f, 106.2f, 107.1f, 107.2f};
  uint64_t c_buffer_coords[] = {1, 1, 1, 2, 1, 4, 2, 3, 3, 1,
                                3, 2, 4, 1, 4, 2, 3, 3, 3, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_size, buffer_coords_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a3", subarray, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, TILEDB_COORDS, subarray, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords = (uint64_t*)malloc(buffer_coords_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
}

void ConsolidationFx::read_sparse_unordered_full() {
  // Correct buffers
  int c_buffer_a1[] = {0, 1, 2, 3, 4, 104, 105, 5, 6, 7};
  uint64_t c_buffer_a2_off[] = {0, 1, 3, 6, 10, 11, 12, 16, 18, 21};
  char c_buffer_a2_val[] = "abbcccddddeuvvvvffggghhhh";
  float c_buffer_a3[] = {0.1f, 0.2f, 1.1f, 1.2f,   2.1f,   2.2f,   3.1f,
                         3.2f, 4.1f, 4.2f, 104.1f, 104.2f, 105.1f, 105.2f,
                         5.1f, 5.2f, 6.1f, 6.2f,   7.1f,   7.2f};
  uint64_t c_buffer_coords[] = {1, 1, 1, 2, 1, 4, 2, 3, 3, 1,
                                3, 2, 4, 1, 4, 2, 3, 3, 3, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_size, buffer_coords_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a3", subarray, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, TILEDB_COORDS, subarray, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords = (uint64_t*)malloc(buffer_coords_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
}

void ConsolidationFx::consolidate_dense() {
  int rc = tiledb_array_consolidate(ctx_, DENSE_ARRAY_NAME);
  CHECK(rc == TILEDB_OK);
}

void ConsolidationFx::consolidate_sparse() {
  int rc = tiledb_array_consolidate(ctx_, SPARSE_ARRAY_NAME);
  CHECK(rc == TILEDB_OK);
}

void ConsolidationFx::remove_array(const std::string& array_name) {
  if (!is_array(array_name))
    return;

  CHECK(tiledb_object_remove(ctx_, array_name.c_str()) == TILEDB_OK);
}

void ConsolidationFx::remove_dense_array() {
  remove_array(DENSE_ARRAY_NAME);
}

void ConsolidationFx::remove_sparse_array() {
  remove_array(SPARSE_ARRAY_NAME);
}

bool ConsolidationFx::is_array(const std::string& array_name) {
  tiledb_object_t type = TILEDB_INVALID;
  REQUIRE(tiledb_object_type(ctx_, array_name.c_str(), &type) == TILEDB_OK);
  return type == TILEDB_ARRAY;
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test consolidation, dense",
    "[capi], [consolidation], [dense-consolidation]") {
  remove_dense_array();
  create_dense_array();

  SECTION("- write full, subarray, unordered") {
    write_dense_full();
    write_dense_subarray();
    write_dense_unordered();
    consolidate_dense();
    read_dense_full_subarray_unordered();
  }

  SECTION("- write subarray, full, unordered") {
    write_dense_subarray();
    write_dense_full();
    write_dense_unordered();
    consolidate_dense();
    read_dense_subarray_full_unordered();
  }

  SECTION("- write subarray, unordered, full") {
    write_dense_subarray();
    write_dense_unordered();
    write_dense_full();
    consolidate_dense();
    read_dense_subarray_unordered_full();
  }

  remove_dense_array();
}

TEST_CASE_METHOD(
    ConsolidationFx,
    "C API: Test consolidation, sparse",
    "[capi], [consolidation], [sparse-consolidation]") {
  remove_sparse_array();
  create_sparse_array();

  SECTION("- write full, unordered") {
    write_sparse_full();
    write_sparse_unordered();
    consolidate_sparse();
    read_sparse_full_unordered();
  }

  SECTION("- write unordered, full") {
    write_sparse_unordered();
    write_sparse_full();
    consolidate_sparse();
    read_sparse_unordered_full();
  }

  remove_sparse_array();
}
