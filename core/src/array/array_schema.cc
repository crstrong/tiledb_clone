/**
 * @file   array_schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file implements the ArraySchema class.
 */

#include "array_schema.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <cassert>
#include <cmath>
#include <iostream>
#include "compressor.h"
#include "configurator.h"
#include "filesystem.h"
#include "logger.h"
#include "utils.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArraySchema::ArraySchema() {
  cell_num_per_tile_ = -1;
  domain_ = nullptr;
  tile_extents_ = nullptr;
  tile_domain_ = nullptr;
  tile_coords_aux_ = nullptr;

  set_default();
}

ArraySchema::~ArraySchema() {
  if (domain_ != nullptr)
    free(domain_);

  if (tile_extents_ != nullptr)
    free(tile_extents_);

  if (tile_domain_ != nullptr)
    free(tile_domain_);

  if (tile_coords_aux_ != nullptr)
    free(tile_coords_aux_);
}

/* ****************************** */
/*            ACCESSORS           */
/* ****************************** */

const std::string& ArraySchema::array_name() const {
  return array_name_;
}

void ArraySchema::array_schema_export(ArraySchemaC* array_schema_c) const {
  // Set array name
  size_t array_name_len = array_name_.size();
  array_schema_c->array_name_ = (char*)malloc(array_name_len + 1);
  strcpy(array_schema_c->array_name_, array_name_.c_str());

  // Set attributes and number of attributes.
  array_schema_c->attribute_num_ = attribute_num_;
  array_schema_c->attributes_ = (char**)malloc(attribute_num_ * sizeof(char*));
  for (int i = 0; i < attribute_num_; ++i) {
    size_t attribute_len = attributes_[i].size();
    array_schema_c->attributes_[i] = (char*)malloc(attribute_len + 1);
    strcpy(array_schema_c->attributes_[i], attributes_[i].c_str());
  }

  // Set dimensions
  array_schema_c->dim_num_ = dim_num_;
  array_schema_c->dimensions_ = (char**)malloc(dim_num_ * sizeof(char*));
  for (int i = 0; i < dim_num_; ++i) {
    size_t dimension_len = dimensions_[i].size();
    array_schema_c->dimensions_[i] = (char*)malloc(dimension_len + 1);
    strcpy(array_schema_c->dimensions_[i], dimensions_[i].c_str());
  }

  // Set dense
  array_schema_c->dense_ = dense_;

  // Set domain
  size_t coords_size = this->coords_size();
  array_schema_c->domain_ = malloc(2 * coords_size);
  memcpy(array_schema_c->domain_, domain_, 2 * coords_size);

  // Set tile extents
  if (tile_extents_ == nullptr) {
    array_schema_c->tile_extents_ = nullptr;
  } else {
    array_schema_c->tile_extents_ = malloc(coords_size);
    memcpy(array_schema_c->tile_extents_, tile_extents_, coords_size);
  }

  // Set types
  array_schema_c->types_ =
      (tiledb_datatype_t*)malloc((attribute_num_ + 1) * sizeof(int));
  for (int i = 0; i < attribute_num_ + 1; ++i)
    array_schema_c->types_[i] = static_cast<tiledb_datatype_t>(types_[i]);

  // Set cell val num
  array_schema_c->cell_val_num_ = (int*)malloc((attribute_num_) * sizeof(int));
  for (int i = 0; i < attribute_num_; ++i)
    array_schema_c->cell_val_num_[i] = cell_val_num_[i];

  // Set cell order
  array_schema_c->cell_order_ = static_cast<tiledb_layout_t>(cell_order_);

  // Set tile order
  array_schema_c->tile_order_ = static_cast<tiledb_layout_t>(tile_order_);

  // Set capacity
  array_schema_c->capacity_ = capacity_;

  // Set compression
  array_schema_c->compressor_ =
      (tiledb_compressor_t*)malloc((attribute_num_ + 1) * sizeof(int));
  for (int i = 0; i < attribute_num_ + 1; ++i)
    array_schema_c->compressor_[i] =
        static_cast<tiledb_compressor_t>(compressor_[i]);
}

void ArraySchema::array_schema_export(
    MetadataSchemaC* metadata_schema_c) const {
  // Set metadata name
  size_t array_name_len = array_name_.size();
  metadata_schema_c->metadata_name_ = (char*)malloc(array_name_len + 1);
  strcpy(metadata_schema_c->metadata_name_, array_name_.c_str());

  // Set attributes and number of attributes
  metadata_schema_c->attribute_num_ = attribute_num_ - 1;
  metadata_schema_c->attributes_ =
      (char**)malloc((attribute_num_ - 1) * sizeof(char*));
  for (int i = 0; i < attribute_num_ - 1; ++i) {
    size_t attribute_len = attributes_[i].size();
    metadata_schema_c->attributes_[i] = (char*)malloc(attribute_len + 1);
    strcpy(metadata_schema_c->attributes_[i], attributes_[i].c_str());
  }

  // Set types
  metadata_schema_c->types_ =
      (tiledb_datatype_t*)malloc((attribute_num_ - 1) * sizeof(int));
  for (int i = 0; i < attribute_num_ - 1; ++i)
    metadata_schema_c->types_[i] = static_cast<tiledb_datatype_t>(types_[i]);

  // Set cell val num
  metadata_schema_c->cell_val_num_ =
      (int*)malloc((attribute_num_ - 1) * sizeof(int));
  for (int i = 0; i < attribute_num_ - 1; ++i)
    metadata_schema_c->cell_val_num_[i] = cell_val_num_[i];

  // Set capacity
  metadata_schema_c->capacity_ = capacity_;

  // Set compression
  metadata_schema_c->compressor_ =
      (tiledb_compressor_t*)malloc(attribute_num_ * sizeof(int));
  for (int i = 0; i < attribute_num_; ++i)
    metadata_schema_c->compressor_[i] =
        static_cast<tiledb_compressor_t>(compressor_[i]);
}

const std::string& ArraySchema::attribute(int attribute_id) const {
  assert(attribute_id >= 0 && attribute_id <= attribute_num_ + 1);

  // Special case for the search attribute (same as coordinates)
  if (attribute_id == attribute_num_ + 1)
    attribute_id = attribute_num_;

  return attributes_[attribute_id];
}

Status ArraySchema::attribute_id(const std::string& attribute, int* id) const {
  // Special case - coordinates
  if (attribute == Configurator::coords()) {
    *id = attribute_num_;
    return Status::Ok();
  }

  for (int i = 0; i < attribute_num_; ++i) {
    if (attributes_[i] == attribute) {
      *id = i;
      return Status::Ok();
    }
  }
  return LOG_STATUS(
      Status::ArraySchemaError("Attribute not found: " + attribute));
}

int ArraySchema::attribute_num() const {
  return attribute_num_;
}

const std::vector<std::string>& ArraySchema::attributes() const {
  return attributes_;
}

int64_t ArraySchema::capacity() const {
  return capacity_;
}

int64_t ArraySchema::cell_num_per_tile() const {
  // Sanity check
  assert(dense_);

  return cell_num_per_tile_;
}

Layout ArraySchema::cell_order() const {
  return cell_order_;
}

size_t ArraySchema::cell_size(int attribute_id) const {
  // Special case for the search tile
  if (attribute_id == attribute_num_ + 1)
    attribute_id = attribute_num_;

  return cell_sizes_[attribute_id];
}

int ArraySchema::cell_val_num(int attribute_id) const {
  return cell_val_num_[attribute_id];
}

Compressor ArraySchema::compression(int attr_id) const {
  assert(attr_id >= 0 && attr_id <= attribute_num_ + 1);
  // Special case for the "search tile", which is essentially the
  // coordinates tile
  if (attr_id == attribute_num_ + 1)
    attr_id = attribute_num_;
  return compressor_.at(attr_id);
}

size_t ArraySchema::coords_size() const {
  return coords_size_;
}

Datatype ArraySchema::coords_type() const {
  return types_[attribute_num_];
}

bool ArraySchema::dense() const {
  return dense_;
}

int ArraySchema::dim_num() const {
  return dim_num_;
}

const void* ArraySchema::domain() const {
  return domain_;
}

Status ArraySchema::get_attribute_ids(
    const std::vector<std::string>& attributes,
    std::vector<int>& attribute_ids) const {
  // Initialization
  attribute_ids.clear();
  int attribute_num = attributes.size();
  int id;

  // Get attribute ids
  for (int i = 0; i < attribute_num; ++i) {
    RETURN_NOT_OK(attribute_id(attributes[i], &id));
    attribute_ids.push_back(id);
  }
  return Status::Ok();
}

bool ArraySchema::is_contained_in_tile_slab_col(const void* range) const {
  Datatype typ = types_.at(attribute_num_);
  switch (typ) {
    case Datatype::INT32:
      return is_contained_in_tile_slab_col(static_cast<const int*>(range));
    case Datatype::INT64:
      return is_contained_in_tile_slab_col(static_cast<const int64_t*>(range));
    case Datatype::FLOAT32:
      return is_contained_in_tile_slab_col(static_cast<const float*>(range));
    case Datatype::FLOAT64:
      return is_contained_in_tile_slab_col(static_cast<const double*>(range));
    case Datatype::INT8:
      return is_contained_in_tile_slab_col(static_cast<const int8_t*>(range));
    case Datatype::UINT8:
      return is_contained_in_tile_slab_col(static_cast<const uint8_t*>(range));
    case Datatype::INT16:
      return is_contained_in_tile_slab_col(static_cast<const int16_t*>(range));
    case Datatype::UINT16:
      return is_contained_in_tile_slab_col(static_cast<const uint16_t*>(range));
    case Datatype::UINT32:
      return is_contained_in_tile_slab_col(static_cast<const uint32_t*>(range));
    case Datatype::UINT64:
      return is_contained_in_tile_slab_col(static_cast<const uint64_t*>(range));
    default:
      return false;
  }
}

bool ArraySchema::is_contained_in_tile_slab_row(const void* range) const {
  Datatype typ = types_.at(attribute_num_);
  switch (typ) {
    case Datatype::INT32:
      return is_contained_in_tile_slab_row(static_cast<const int*>(range));
    case Datatype::INT64:
      return is_contained_in_tile_slab_row(static_cast<const int64_t*>(range));
    case Datatype::FLOAT32:
      return is_contained_in_tile_slab_row(static_cast<const float*>(range));
    case Datatype::FLOAT64:
      return is_contained_in_tile_slab_row(static_cast<const double*>(range));
    case Datatype::INT8:
      return is_contained_in_tile_slab_row(static_cast<const int8_t*>(range));
    case Datatype::UINT8:
      return is_contained_in_tile_slab_row(static_cast<const uint8_t*>(range));
    case Datatype::INT16:
      return is_contained_in_tile_slab_row(static_cast<const int16_t*>(range));
    case Datatype::UINT16:
      return is_contained_in_tile_slab_row(static_cast<const uint16_t*>(range));
    case Datatype::UINT32:
      return is_contained_in_tile_slab_row(static_cast<const uint32_t*>(range));
    case Datatype::UINT64:
      return is_contained_in_tile_slab_row(static_cast<const uint64_t*>(range));
    default:
      return false;
  }
}

void ArraySchema::print() const {
  // Array name
  std::cout << "Array name:\n\t" << array_name_ << "\n";
  // Dimension names
  std::cout << "Dimension names:\n";
  for (int i = 0; i < dim_num_; ++i)
    std::cout << "\t" << dimensions_[i] << "\n";
  // Attribute names
  std::cout << "Attribute names:\n";
  for (int i = 0; i < attribute_num_; ++i)
    std::cout << "\t" << attributes_[i] << "\n";
  // Domain
  std::cout << "Domain:\n";
  Datatype typ = types_.at(attribute_num_);
  if (typ == Datatype::INT32) {
    int* domain_int = (int*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      std::cout << "\t" << dimensions_[i] << ": [" << domain_int[2 * i] << ","
                << domain_int[2 * i + 1] << "]\n";
    }
  } else if (typ == Datatype::INT64) {
    int64_t* domain_int64 = (int64_t*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      std::cout << "\t" << dimensions_[i] << ": [" << domain_int64[2 * i] << ","
                << domain_int64[2 * i + 1] << "]\n";
    }
  } else if (typ == Datatype::FLOAT32) {
    float* domain_float = (float*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      std::cout << "\t" << dimensions_[i] << ": [" << domain_float[2 * i] << ","
                << domain_float[2 * i + 1] << "]\n";
    }
  } else if (typ == Datatype::FLOAT64) {
    double* domain_double = (double*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      std::cout << "\t" << dimensions_[i] << ": [" << domain_double[2 * i]
                << "," << domain_double[2 * i + 1] << "]\n";
    }
  } else if (typ == Datatype::INT8) {
    int8_t* domain_int8 = (int8_t*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      std::cout << "\t" << dimensions_[i] << ": [" << domain_int8[2 * i] << ","
                << domain_int8[2 * i + 1] << "]\n";
    }
  } else if (typ == Datatype::UINT8) {
    uint8_t* domain_uint8 = (uint8_t*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      std::cout << "\t" << dimensions_[i] << ": [" << domain_uint8[2 * i] << ","
                << domain_uint8[2 * i + 1] << "]\n";
    }
  } else if (typ == Datatype::INT16) {
    int16_t* domain_int16 = (int16_t*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      std::cout << "\t" << dimensions_[i] << ": [" << domain_int16[2 * i] << ","
                << domain_int16[2 * i + 1] << "]\n";
    }
  } else if (typ == Datatype::UINT16) {
    uint16_t* domain_uint16 = (uint16_t*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      std::cout << "\t" << dimensions_[i] << ": [" << domain_uint16[2 * i]
                << "," << domain_uint16[2 * i + 1] << "]\n";
    }
  } else if (typ == Datatype::UINT32) {
    uint32_t* domain_uint32 = (uint32_t*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      std::cout << "\t" << dimensions_[i] << ": [" << domain_uint32[2 * i]
                << "," << domain_uint32[2 * i + 1] << "]\n";
    }
  } else if (typ == Datatype::UINT64) {
    uint64_t* domain_uint64 = (uint64_t*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      std::cout << "\t" << dimensions_[i] << ": [" << domain_uint64[2 * i]
                << "," << domain_uint64[2 * i + 1] << "]\n";
    }
  }
  // Types
  std::cout << "Types:\n";
  for (int i = 0; i < attribute_num_; ++i) {
    typ = types_.at(i);
    if (typ == Datatype::CHAR) {
      std::cout << "\t" << attributes_[i] << ": char[";
    } else if (typ == Datatype::INT32) {
      std::cout << "\t" << attributes_[i] << ": int32[";
    } else if (typ == Datatype::INT64) {
      std::cout << "\t" << attributes_[i] << ": int64[";
    } else if (typ == Datatype::FLOAT32) {
      std::cout << "\t" << attributes_[i] << ": float32[";
    } else if (typ == Datatype::FLOAT64) {
      std::cout << "\t" << attributes_[i] << ": float64[";
    } else if (typ == Datatype::INT8) {
      std::cout << "\t" << attributes_[i] << ": int8[";
    } else if (typ == Datatype::UINT8) {
      std::cout << "\t" << attributes_[i] << ": uint8[";
    } else if (typ == Datatype::INT16) {
      std::cout << "\t" << attributes_[i] << ": int16[";
    } else if (typ == Datatype::UINT16) {
      std::cout << "\t" << attributes_[i] << ": uint16[";
    } else if (typ == Datatype::UINT32) {
      std::cout << "\t" << attributes_[i] << ": uint32[";
    } else if (typ == Datatype::UINT64) {
      std::cout << "\t" << attributes_[i] << ": uint64[";
    }
    if (cell_val_num_[i] == Configurator::var_num())
      std::cout << "var]\n";
    else
      std::cout << cell_val_num_[i] << "]\n";
  }
  typ = types_.at(attribute_num_);
  if (typ == Datatype::INT32)
    std::cout << "\tCoordinates: int32\n";
  else if (typ == Datatype::INT64)
    std::cout << "\tCoordinates: int64\n";
  else if (typ == Datatype::FLOAT32)
    std::cout << "\tCoordinates: float32\n";
  else if (typ == Datatype::FLOAT64)
    std::cout << "\tCoordinates: float64\n";
  else if (typ == Datatype::INT8)
    std::cout << "\tCoordinates: int8\n";
  else if (typ == Datatype::UINT8)
    std::cout << "\tCoordinates: uint8\n";
  else if (typ == Datatype::INT16)
    std::cout << "\tCoordinates: int16\n";
  else if (typ == Datatype::UINT16)
    std::cout << "\tCoordinates: uint16\n";
  else if (typ == Datatype::UINT32)
    std::cout << "\tCoordinates: uint32\n";
  else if (typ == Datatype::UINT64)
    std::cout << "\tCoordinates: uint64\n";
  // Cell sizes
  std::cout << "Cell sizes (in bytes):\n";
  for (int i = 0; i <= attribute_num_; ++i) {
    std::cout << "\t"
              << ((i == attribute_num_) ? "Coordinates" : attributes_[i])
              << ": ";
    if (cell_sizes_[i] == Configurator::var_size())
      std::cout << "var\n";
    else
      std::cout << cell_sizes_[i] << "\n";
  }
  // Dense
  std::cout << "Dense:\n\t" << (dense_ ? "true" : "false") << "\n";
  // Tile type
  std::cout << "Tile types:\n\t"
            << (tile_extents_ == nullptr ? "irregular" : "regular") << "\n";
  // Tile order
  std::cout << "Tile order:\n\t";
  if (tile_extents_ == nullptr) {
    std::cout << "-\n";
  } else {
    if (tile_order_ == Layout::COL_MAJOR)
      std::cout << "column-major\n";
    else if (tile_order_ == Layout::ROW_MAJOR)
      std::cout << "row-major\n";
  }
  // Cell order
  std::cout << "Cell order:\n\t";
  if (cell_order_ == Layout::COL_MAJOR)
    std::cout << "column-major\n";
  else if (cell_order_ == Layout::ROW_MAJOR)
    std::cout << "row-major\n";
  // Capacity
  std::cout << "Capacity:\n\t";
  if (tile_extents_ != nullptr)
    std::cout << "-\n";
  else
    std::cout << capacity_ << "\n";
  // Tile extents
  std::cout << "Tile extents:\n";
  if (tile_extents_ == nullptr) {
    std::cout << "-\n";
  } else {
    typ = types_.at(attribute_num_);
    if (typ == Datatype::INT32) {
      int* tile_extents_int = (int*)tile_extents_;
      for (int i = 0; i < dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " << tile_extents_int[i]
                  << "\n";
    } else if (typ == Datatype::INT64) {
      int64_t* tile_extents_int64 = (int64_t*)tile_extents_;
      for (int i = 0; i < dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " << tile_extents_int64[i]
                  << "\n";
    } else if (typ == Datatype::FLOAT32) {
      float* tile_extents_float = (float*)tile_extents_;
      for (int i = 0; i < dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " << tile_extents_float[i]
                  << "\n";
    } else if (typ == Datatype::FLOAT64) {
      double* tile_extents_double = (double*)tile_extents_;
      for (int i = 0; i < dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " << tile_extents_double[i]
                  << "\n";
    } else if (typ == Datatype::INT8) {
      int8_t* tile_extents_int8 = (int8_t*)tile_extents_;
      for (int i = 0; i < dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " << tile_extents_int8[i]
                  << "\n";
    } else if (typ == Datatype::UINT8) {
      uint8_t* tile_extents_uint8 = (uint8_t*)tile_extents_;
      for (int i = 0; i < dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " << tile_extents_uint8[i]
                  << "\n";
    } else if (typ == Datatype::INT16) {
      int16_t* tile_extents_int16 = (int16_t*)tile_extents_;
      for (int i = 0; i < dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " << tile_extents_int16[i]
                  << "\n";
    } else if (typ == Datatype::UINT16) {
      uint16_t* tile_extents_uint16 = (uint16_t*)tile_extents_;
      for (int i = 0; i < dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " << tile_extents_uint16[i]
                  << "\n";
    } else if (typ == Datatype::UINT32) {
      uint32_t* tile_extents_uint32 = (uint32_t*)tile_extents_;
      for (int i = 0; i < dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " << tile_extents_uint32[i]
                  << "\n";
    } else if (typ == Datatype::UINT64) {
      uint64_t* tile_extents_uint64 = (uint64_t*)tile_extents_;
      for (int i = 0; i < dim_num_; ++i)
        std::cout << "\t" << dimensions_[i] << ": " << tile_extents_uint64[i]
                  << "\n";
    }
  }
  // Compression type
  std::cout << "Compression type:\n";
  for (int i = 0; i < attribute_num_; ++i)
    if (compressor_[i] == Compressor::GZIP)
      std::cout << "\t" << attributes_[i] << ": GZIP\n";
    else if (compressor_[i] == Compressor::ZSTD)
      std::cout << "\t" << attributes_[i] << ": ZSTD\n";
    else if (compressor_[i] == Compressor::LZ4)
      std::cout << "\t" << attributes_[i] << ": LZ4\n";
    else if (compressor_[i] == Compressor::BLOSC)
      std::cout << "\t" << attributes_[i] << ": BLOSC\n";
    else if (compressor_[i] == Compressor::BLOSC_LZ4)
      std::cout << "\t" << attributes_[i] << ": BLOSC_LZ4\n";
    else if (compressor_[i] == Compressor::BLOSC_LZ4HC)
      std::cout << "\t" << attributes_[i] << ": BLOSC_LZ4HC\n";
    else if (compressor_[i] == Compressor::BLOSC_SNAPPY)
      std::cout << "\t" << attributes_[i] << ": BLOSC_SNAPPY\n";
    else if (compressor_[i] == Compressor::BLOSC_ZLIB)
      std::cout << "\t" << attributes_[i] << ": BLOSC_ZLIB\n";
    else if (compressor_[i] == Compressor::BLOSC_ZSTD)
      std::cout << "\t" << attributes_[i] << ": BLOSC_ZSTD\n";
    else if (compressor_[i] == Compressor::RLE)
      std::cout << "\t" << attributes_[i] << ": RLE\n";
    else if (compressor_[i] == Compressor::BZIP2)
      std::cout << "\t" << attributes_[i] << ": BZIP2\n";
    else if (compressor_[i] == Compressor::NO_COMPRESSION)
      std::cout << "\t" << attributes_[i] << ": NONE\n";
  if (compressor_[attribute_num_] == Compressor::GZIP)
    std::cout << "\tCoordinates: GZIP\n";
  else if (compressor_[attribute_num_] == Compressor::ZSTD)
    std::cout << "\tCoordinates: ZSTD\n";
  else if (compressor_[attribute_num_] == Compressor::LZ4)
    std::cout << "\tCoordinates: LZ4\n";
  else if (compressor_[attribute_num_] == Compressor::BLOSC)
    std::cout << "\tCoordinates: BLOSC\n";
  else if (compressor_[attribute_num_] == Compressor::BLOSC_LZ4)
    std::cout << "\tCoordinates: BLOSC_LZ4\n";
  else if (compressor_[attribute_num_] == Compressor::BLOSC_LZ4HC)
    std::cout << "\tCoordinates: BLOSC_LZ4HC\n";
  else if (compressor_[attribute_num_] == Compressor::BLOSC_SNAPPY)
    std::cout << "\tCoordinates: BLOSC_SNAPPY\n";
  else if (compressor_[attribute_num_] == Compressor::BLOSC_ZLIB)
    std::cout << "\tCoordinates: BLOSC_ZLIB\n";
  else if (compressor_[attribute_num_] == Compressor::BLOSC_ZSTD)
    std::cout << "\tCoordinates: BLOSC_ZSTD\n";
  else if (compressor_[attribute_num_] == Compressor::RLE)
    std::cout << "\tCoordinates: RLE\n";
  else if (compressor_[attribute_num_] == Compressor::BZIP2)
    std::cout << "\tCoordinates: BZIP2\n";
  else if (compressor_[attribute_num_] == Compressor::NO_COMPRESSION)
    std::cout << "\tCoordinates: NONE\n";
}

// ===== FORMAT =====
// array_name_size(int)
//     array_name(string)
// dense(bool)
// tile_order(char)
// cell_order(char)
// capacity(int64_t)
// attribute_num(int)
//     attribute_size#1(int) attribute#1(string)
//     attribute_size#2(int) attribute#2(string)
//     ...
// dim_num(int)
//    dimension_size#1(int) dimension#1(string)
//    dimension_size#2(int) dimension#2(string)
//    ...
// domain_size(int)
// domain#1_low(double) dim_domain#1_high(double)
// domain#2_low(double) dim_domain#2_high(double)
//  ...
// tile_extents_size(int)
//     tile_extent#1(double) tile_extent#2(double) ...
// type#1(char) type#2(char) ...
// cell_val_num#1(int) cell_val_num#2(int) ...
// compression#1(char) compression#2(char) ...
Status ArraySchema::serialize(
    void*& array_schema_bin, size_t& array_schema_bin_size) const {
  // Compute the size of the binary representation of the ArraySchema object
  array_schema_bin_size = compute_bin_size();

  // Allocate memory
  array_schema_bin = malloc(array_schema_bin_size);

  // For easy reference
  char* buffer = (char*)array_schema_bin;
  size_t buffer_size = array_schema_bin_size;
  size_t offset = 0;

  // Copy array_name_
  int array_name_size = array_name_.size();
  assert(offset + sizeof(int) < buffer_size);
  memcpy(buffer + offset, &array_name_size, sizeof(int));
  offset += sizeof(int);
  assert(offset + array_name_size < buffer_size);
  memcpy(buffer + offset, &array_name_[0], array_name_size);
  offset += array_name_size;
  // Copy dense_
  assert(offset + sizeof(bool) < buffer_size);
  memcpy(buffer + offset, &dense_, sizeof(bool));
  offset += sizeof(bool);
  // Copy tile_order_
  char tile_order = static_cast<char>(tile_order_);
  assert(offset + sizeof(char) < buffer_size);
  memcpy(buffer + offset, &tile_order, sizeof(char));
  offset += sizeof(char);
  // Copy cell_order_
  char cell_order = static_cast<char>(cell_order_);
  assert(offset + sizeof(char) < buffer_size);
  memcpy(buffer + offset, &cell_order, sizeof(char));
  offset += sizeof(char);
  // Copy capacity_
  assert(offset + sizeof(int64_t) < buffer_size);
  memcpy(buffer + offset, &capacity_, sizeof(int64_t));
  offset += sizeof(int64_t);
  // Copy attributes_
  assert(offset + sizeof(int) < buffer_size);
  memcpy(buffer + offset, &attribute_num_, sizeof(int));
  offset += sizeof(int);
  int attribute_size;
  for (int i = 0; i < attribute_num_; i++) {
    attribute_size = attributes_[i].size();
    assert(offset + sizeof(int) < buffer_size);
    memcpy(buffer + offset, &attribute_size, sizeof(int));
    offset += sizeof(int);
    assert(offset + attribute_size < buffer_size);
    memcpy(buffer + offset, attributes_[i].c_str(), attribute_size);
    offset += attribute_size;
  }
  // Copy dimensions_
  assert(offset + sizeof(int) < buffer_size);
  memcpy(buffer + offset, &dim_num_, sizeof(int));
  offset += sizeof(int);
  int dimension_size;
  for (int i = 0; i < dim_num_; i++) {
    dimension_size = dimensions_[i].size();
    assert(offset + sizeof(int) < buffer_size);
    memcpy(buffer + offset, &dimension_size, sizeof(int));
    offset += sizeof(int);
    assert(offset + dimension_size < buffer_size);
    memcpy(buffer + offset, dimensions_[i].c_str(), dimension_size);
    offset += dimension_size;
  }
  // Copy domain_
  int domain_size = 2 * coords_size();
  assert(offset + sizeof(int) < buffer_size);
  memcpy(buffer + offset, &domain_size, sizeof(int));
  offset += sizeof(int);
  assert(offset + domain_size < buffer_size);
  memcpy(buffer + offset, domain_, domain_size);
  offset += 2 * coords_size();
  // Copy tile_extents_
  int tile_extents_size = ((tile_extents_ == nullptr) ? 0 : coords_size());
  assert(offset + sizeof(int) < buffer_size);
  memcpy(buffer + offset, &tile_extents_size, sizeof(int));
  offset += sizeof(int);
  if (tile_extents_ != nullptr) {
    assert(offset + tile_extents_size < buffer_size);
    memcpy(buffer + offset, tile_extents_, tile_extents_size);
    offset += tile_extents_size;
  }
  // Copy types_
  char type;
  for (int i = 0; i <= attribute_num_; i++) {
    type = static_cast<char>(types_[i]);
    assert(offset + sizeof(char) < buffer_size);
    memcpy(buffer + offset, &type, sizeof(char));
    offset += sizeof(char);
  }
  // Copy cell_val_num_
  for (int i = 0; i < attribute_num_; i++) {
    assert(offset + sizeof(int) < buffer_size);
    memcpy(buffer + offset, &cell_val_num_[i], sizeof(int));
    offset += sizeof(int);
  }
  // Copy compressor_
  char compression;
  for (int i = 0; i <= attribute_num_; ++i) {
    compression = static_cast<char>(compressor_[i]);
    assert(offset + sizeof(char) <= buffer_size);
    memcpy(buffer + offset, &compression, sizeof(char));
    offset += sizeof(char);
  }
  assert(offset == buffer_size);

  return Status::Ok();
}

Status ArraySchema::store(const std::string& dir) const {
  // Open array schema file
  std::string filename = dir + "/" + Configurator::array_schema_filename();
  remove(filename.c_str());
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  if (fd == -1)
    return LOG_STATUS(Status::ArraySchemaError(
        std::string("Cannot store schema; ") + strerror(errno)));

  // Serialize array schema
  void* array_schema_bin;
  size_t array_schema_bin_size;
  RETURN_NOT_OK(serialize(array_schema_bin, array_schema_bin_size));

  // Store the array schema
  ssize_t bytes_written = ::write(fd, array_schema_bin, array_schema_bin_size);
  if (bytes_written != ssize_t(array_schema_bin_size)) {
    free(array_schema_bin);
    return LOG_STATUS(Status::ArraySchemaError(
        std::string("Cannot store schema; ") + strerror(errno)));
  }

  // Clean up
  free(array_schema_bin);
  if (::close(fd))
    return LOG_STATUS(Status::ArraySchemaError(
        std::string("Cannot store schema; ") + strerror(errno)));

  // Success
  return Status::Ok();
}

template <class T>
int ArraySchema::subarray_overlap(
    const T* subarray_a, const T* subarray_b, T* overlap_subarray) const {
  // Get overlap range
  for (int i = 0; i < dim_num_; ++i) {
    overlap_subarray[2 * i] = MAX(subarray_a[2 * i], subarray_b[2 * i]);
    overlap_subarray[2 * i + 1] =
        MIN(subarray_a[2 * i + 1], subarray_b[2 * i + 1]);
  }

  // Check overlap
  int overlap = 1;
  for (int i = 0; i < dim_num_; ++i) {
    if (overlap_subarray[2 * i] > subarray_b[2 * i + 1] ||
        overlap_subarray[2 * i + 1] < subarray_b[2 * i]) {
      overlap = 0;
      break;
    }
  }

  // Check partial overlap
  if (overlap == 1) {
    for (int i = 0; i < dim_num_; ++i) {
      if (overlap_subarray[2 * i] != subarray_b[2 * i] ||
          overlap_subarray[2 * i + 1] != subarray_b[2 * i + 1]) {
        overlap = 2;
        break;
      }
    }
  }

  // Check contig overlap
  if (overlap == 2) {
    overlap = 3;
    if (cell_order_ == Layout::ROW_MAJOR) {  // Row major
      for (int i = 1; i < dim_num_; ++i) {
        if (overlap_subarray[2 * i] != subarray_b[2 * i] ||
            overlap_subarray[2 * i + 1] != subarray_b[2 * i + 1]) {
          overlap = 2;
          break;
        }
      }
    } else if (cell_order_ == Layout::COL_MAJOR) {  // Column major
      for (int i = dim_num_ - 2; i >= 0; --i) {
        if (overlap_subarray[2 * i] != subarray_b[2 * i] ||
            overlap_subarray[2 * i + 1] != subarray_b[2 * i + 1]) {
          overlap = 2;
          break;
        }
      }
    }
  }

  // Return
  return overlap;
}

const void* ArraySchema::tile_domain() const {
  return tile_domain_;
}

const void* ArraySchema::tile_extents() const {
  return tile_extents_;
}

int64_t ArraySchema::tile_num() const {
  // Invoke the proper template function
  Datatype typ = types_.at(attribute_num_);
  switch (typ) {
    case Datatype::INT32:
      return tile_num<int>();
    case Datatype::INT64:
      return tile_num<int64_t>();
    case Datatype::INT8:
      return tile_num<int8_t>();
    case Datatype::UINT8:
      return tile_num<uint8_t>();
    case Datatype::INT16:
      return tile_num<int16_t>();
    case Datatype::UINT16:
      return tile_num<uint16_t>();
    case Datatype::UINT32:
      return tile_num<uint32_t>();
    case Datatype::UINT64:
      return tile_num<uint64_t>();
    default:
      assert(0);
      return -1;
  }
}

template <class T>
int64_t ArraySchema::tile_num() const {
  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  int64_t ret = 1;
  for (int i = 0; i < dim_num_; ++i)
    ret *= (domain[2 * i + 1] - domain[2 * i] + 1) / tile_extents[i];

  return ret;
}

int64_t ArraySchema::tile_num(const void* range) const {
  // Invoke the proper template function
  Datatype typ = types_.at(attribute_num_);
  switch (typ) {
    case Datatype::INT32:
      return tile_num<int>(static_cast<const int*>(range));
    case Datatype::INT64:
      return tile_num<int64_t>(static_cast<const int64_t*>(range));
    case Datatype::INT8:
      return tile_num<int8_t>(static_cast<const int8_t*>(range));
    case Datatype::UINT8:
      return tile_num<uint8_t>(static_cast<const uint8_t*>(range));
    case Datatype::INT16:
      return tile_num<int16_t>(static_cast<const int16_t*>(range));
    case Datatype::UINT16:
      return tile_num<uint16_t>(static_cast<const uint16_t*>(range));
    case Datatype::UINT32:
      return tile_num<uint32_t>(static_cast<const uint32_t*>(range));
    case Datatype::UINT64:
      return tile_num<uint64_t>(static_cast<const uint64_t*>(range));
    default:
      assert(0);
      return -1;
  }
}

template <class T>
int64_t ArraySchema::tile_num(const T* range) const {
  // For easy reference
  const T* tile_extents = static_cast<const T*>(tile_extents_);
  const T* domain = static_cast<const T*>(domain_);

  int64_t ret = 1;
  int64_t start, end;
  for (int i = 0; i < dim_num_; ++i) {
    start = (range[2 * i] - domain[2 * i]) / tile_extents[i];
    end = (range[2 * i + 1] - domain[2 * i]) / tile_extents[i];
    ret *= (end - start + 1);
  }

  return ret;
}

Layout ArraySchema::tile_order() const {
  return tile_order_;
}

int64_t ArraySchema::tile_slab_col_cell_num(const void* subarray) const {
  // Invoke the proper templated function
  Datatype typ = types_.at(attribute_num_);
  switch (typ) {
    case Datatype::INT32:
      return tile_slab_col_cell_num(static_cast<const int*>(subarray));
    case Datatype::INT64:
      return tile_slab_col_cell_num(static_cast<const int64_t*>(subarray));
    case Datatype::FLOAT32:
      return tile_slab_col_cell_num(static_cast<const float*>(subarray));
    case Datatype::FLOAT64:
      return tile_slab_col_cell_num(static_cast<const double*>(subarray));
    case Datatype::INT8:
      return tile_slab_col_cell_num(static_cast<const int8_t*>(subarray));
    case Datatype::UINT8:
      return tile_slab_col_cell_num(static_cast<const uint8_t*>(subarray));
    case Datatype::INT16:
      return tile_slab_col_cell_num(static_cast<const int16_t*>(subarray));
    case Datatype::UINT16:
      return tile_slab_col_cell_num(static_cast<const uint16_t*>(subarray));
    case Datatype::UINT32:
      return tile_slab_col_cell_num(static_cast<const uint32_t*>(subarray));
    case Datatype::UINT64:
      return tile_slab_col_cell_num(static_cast<const uint64_t*>(subarray));
    default:
      assert(0);
      return -1;
  }
}

int64_t ArraySchema::tile_slab_row_cell_num(const void* subarray) const {
  // Invoke the proper templated function
  Datatype typ = types_.at(attribute_num_);
  switch (typ) {
    case Datatype::INT32:
      return tile_slab_row_cell_num(static_cast<const int*>(subarray));
    case Datatype::INT64:
      return tile_slab_row_cell_num(static_cast<const int64_t*>(subarray));
    case Datatype::FLOAT32:
      return tile_slab_row_cell_num(static_cast<const float*>(subarray));
    case Datatype::FLOAT64:
      return tile_slab_row_cell_num(static_cast<const double*>(subarray));
    case Datatype::INT8:
      return tile_slab_row_cell_num(static_cast<const int8_t*>(subarray));
    case Datatype::UINT8:
      return tile_slab_row_cell_num(static_cast<const uint8_t*>(subarray));
    case Datatype::INT16:
      return tile_slab_row_cell_num(static_cast<const int16_t*>(subarray));
    case Datatype::UINT16:
      return tile_slab_row_cell_num(static_cast<const uint16_t*>(subarray));
    case Datatype::UINT32:
      return tile_slab_row_cell_num(static_cast<const uint32_t*>(subarray));
    case Datatype::UINT64:
      return tile_slab_row_cell_num(static_cast<const uint64_t*>(subarray));
    default:
      assert(0);
      return -1;
  }
}

Datatype ArraySchema::type(int i) const {
  if (i < 0 || i > attribute_num_) {
    LOG_ERROR("Cannot retrieve type; Invalid attribute id");
    assert(0);
  }
  return types_[i];
}

size_t ArraySchema::type_size(int i) const {
  assert(i >= 0 && i <= attribute_num_);

  return type_sizes_[i];
}

int ArraySchema::var_attribute_num() const {
  int var_attribute_num = 0;
  for (int i = 0; i < attribute_num_; ++i)
    if (var_size(i))
      ++var_attribute_num;

  return var_attribute_num;
}

bool ArraySchema::var_size(int attribute_id) const {
  return cell_sizes_[attribute_id] == Configurator::var_size();
}

/* ****************************** */
/*             MUTATORS           */
/* ****************************** */

// ===== FORMAT =====
// array_name_size(int)
//     array_name(string)
// dense(bool)
// tile_order(char)
// cell_order(char)
// capacity(int64_t)
// attribute_num(int)
//     attribute_size#1(int) attribute#1(string)
//     attribute_size#2(int) attribute#2(string)
//     ...
// dim_num(int)
//    dimension_size#1(int) dimension#1(string)
//    dimension_size#2(int) dimension#2(string)
//    ...
// domain_size(int)
// domain#1_low(double) dim_domain#1_high(double)
// domain#2_low(double) dim_domain#2_high(double)
//  ...
// tile_extents_size(int)
//     tile_extent#1(double) tile_extent#2(double) ...
// type#1(char) type#2(char) ...
// cell_val_num#1(int) cell_val_num#2(int) ...
// compression#1(char) compression#2(char) ...
Status ArraySchema::deserialize(
    const void* array_schema_bin, size_t array_schema_bin_size) {
  // For easy reference
  const char* buffer = static_cast<const char*>(array_schema_bin);
  size_t buffer_size = array_schema_bin_size;
  size_t offset = 0;

  // Load array_name_
  int array_name_size;
  assert(offset + sizeof(int) < buffer_size);
  memcpy(&array_name_size, buffer + offset, sizeof(int));
  offset += sizeof(int);
  array_name_.resize(array_name_size);
  assert(offset + array_name_size < buffer_size);
  memcpy(&array_name_[0], buffer + offset, array_name_size);
  offset += array_name_size;

  // Load dense_
  assert(offset + sizeof(bool) < buffer_size);
  memcpy(&dense_, buffer + offset, sizeof(bool));
  offset += sizeof(bool);

  // Load tile_order_
  char tile_order;
  assert(offset + sizeof(char) < buffer_size);
  memcpy(&tile_order, buffer + offset, sizeof(char));
  tile_order_ = static_cast<Layout>(tile_order);
  offset += sizeof(char);
  // Load cell_order_
  char cell_order;
  assert(offset + sizeof(char) < buffer_size);
  memcpy(&cell_order, buffer + offset, sizeof(char));
  cell_order_ = static_cast<Layout>(cell_order);
  offset += sizeof(char);
  // Load capacity_
  assert(offset + sizeof(int64_t) < buffer_size);
  memcpy(&capacity_, buffer + offset, sizeof(int64_t));
  offset += sizeof(int64_t);
  // Load attributes_
  assert(offset + sizeof(int) < buffer_size);
  memcpy(&attribute_num_, buffer + offset, sizeof(int));
  offset += sizeof(int);
  attributes_.resize(attribute_num_);
  int attribute_size;
  for (int i = 0; i < attribute_num_; ++i) {
    assert(offset + sizeof(int) < buffer_size);
    memcpy(&attribute_size, buffer + offset, sizeof(int));
    offset += sizeof(int);
    attributes_[i].resize(attribute_size);
    assert(offset + attribute_size < buffer_size);
    memcpy(&attributes_[i][0], buffer + offset, attribute_size);
    offset += attribute_size;
  }

  // Load dimensions_
  assert(offset + sizeof(int) < buffer_size);
  memcpy(&dim_num_, buffer + offset, sizeof(int));
  offset += sizeof(int);
  dimensions_.resize(dim_num_);
  int dimension_size;
  for (int i = 0; i < dim_num_; ++i) {
    assert(offset + sizeof(int) < buffer_size);
    memcpy(&dimension_size, buffer + offset, sizeof(int));
    offset += sizeof(int);
    dimensions_[i].resize(dimension_size);
    assert(offset + dimension_size < buffer_size);
    memcpy(&dimensions_[i][0], buffer + offset, dimension_size);
    offset += dimension_size;
  }
  // Load domain_
  int domain_size;
  assert(offset + sizeof(int) < buffer_size);
  memcpy(&domain_size, buffer + offset, sizeof(int));
  offset += sizeof(int);
  assert(offset + domain_size < buffer_size);
  domain_ = malloc(domain_size);
  memcpy(domain_, buffer + offset, domain_size);
  offset += domain_size;
  // Load tile_extents_
  int tile_extents_size;
  assert(offset + sizeof(int) < buffer_size);
  memcpy(&tile_extents_size, buffer + offset, sizeof(int));
  offset += sizeof(int);
  if (tile_extents_size == 0) {
    tile_extents_ = nullptr;
  } else {
    assert(offset + tile_extents_size < buffer_size);
    tile_extents_ = malloc(tile_extents_size);
    memcpy(tile_extents_, buffer + offset, tile_extents_size);
    offset += tile_extents_size;
  }

  // Load types_ and set type sizes
  char type;
  types_.resize(attribute_num_ + 1);
  type_sizes_.resize(attribute_num_ + 1);
  for (int i = 0; i <= attribute_num_; ++i) {
    assert(offset + sizeof(char) < buffer_size);
    memcpy(&type, buffer + offset, sizeof(char));
    offset += sizeof(char);
    types_[i] = static_cast<Datatype>(type);
    type_sizes_[i] = compute_type_size(i);
  }
  // Load cell_val_num_
  cell_val_num_.resize(attribute_num_);
  for (int i = 0; i < attribute_num_; ++i) {
    assert(offset + sizeof(int) < buffer_size);
    memcpy(&cell_val_num_[i], buffer + offset, sizeof(int));
    offset += sizeof(int);
  }
  // Load compressor
  char compression;
  for (int i = 0; i <= attribute_num_; ++i) {
    assert(offset + sizeof(char) <= buffer_size);
    memcpy(&compression, buffer + offset, sizeof(char));
    offset += sizeof(char);
    compressor_.push_back(static_cast<Compressor>(compression));
  }
  assert(offset == buffer_size);
  // Add extra coordinate attribute
  attributes_.emplace_back(Configurator::coords());
  // Set cell sizes
  cell_sizes_.resize(attribute_num_ + 1);
  for (int i = 0; i <= attribute_num_; ++i)
    cell_sizes_[i] = compute_cell_size(i);
  // Set coordinates size
  coords_size_ = cell_sizes_[attribute_num_];

  // Compute number of cells per tile
  compute_cell_num_per_tile();

  // Compute tile domain
  compute_tile_domain();

  // Compute tile offsets
  compute_tile_offsets();

  // Initialize static auxiliary variables
  if (tile_coords_aux_ != nullptr)
    free(tile_coords_aux_);
  tile_coords_aux_ = malloc(coords_size_ * dim_num_);

  // Success
  return Status::Ok();
}

Status ArraySchema::init() {
  // Perform check of all members
  // TODO

  // Compute number of cells per tile
  compute_cell_num_per_tile();

  // Compute tile domain
  compute_tile_domain();

  // Compute tile offsets
  compute_tile_offsets();

  // Initialize static auxiliary variables
  if (tile_coords_aux_ != nullptr)
    free(tile_coords_aux_);
  tile_coords_aux_ = malloc(coords_size_ * dim_num_);

  // Success
  return Status::Ok();
}

Status ArraySchema::init(const ArraySchemaC* array_schema_c) {
  // Clear all members
  clear();

  // Set array name
  set_array_name(array_schema_c->array_name_);
  // Set attributes
  RETURN_NOT_OK(set_attributes(
      array_schema_c->attributes_, array_schema_c->attribute_num_));
  // Set capacity
  set_capacity(array_schema_c->capacity_);
  // Set dimensions
  RETURN_NOT_OK(
      set_dimensions(array_schema_c->dimensions_, array_schema_c->dim_num_));
  // Set compression
  RETURN_NOT_OK(set_compression(array_schema_c->compressor_));
  // Set dense
  set_dense(array_schema_c->dense_);
  // Set number of values per cell
  set_cell_val_num(array_schema_c->cell_val_num_);
  // Set types
  RETURN_NOT_OK(set_types(array_schema_c->types_));
  // Set tile extents
  RETURN_NOT_OK(set_tile_extents(array_schema_c->tile_extents_));
  // Set cell order
  RETURN_NOT_OK(set_cell_order(array_schema_c->cell_order_));
  // Set tile order
  RETURN_NOT_OK(set_tile_order(array_schema_c->tile_order_));
  // Set domain
  RETURN_NOT_OK(set_domain(array_schema_c->domain_));

  // Compute number of cells per tile
  compute_cell_num_per_tile();

  // Compute tile domain
  compute_tile_domain();

  // Compute tile offsets
  compute_tile_offsets();

  // Initialize static auxiliary variables
  if (tile_coords_aux_ != nullptr)
    free(tile_coords_aux_);
  tile_coords_aux_ = malloc(coords_size_ * dim_num_);

  return Status::Ok();
}

Status ArraySchema::init(const MetadataSchemaC* metadata_schema_c) {
  // Clear all members
  clear();

  // Create an array schema C struct and populate it
  ArraySchemaC array_schema_c;
  array_schema_c.array_name_ = metadata_schema_c->metadata_name_;
  array_schema_c.capacity_ = metadata_schema_c->capacity_;
  array_schema_c.cell_order_ = TILEDB_ROW_MAJOR;
  array_schema_c.tile_order_ = TILEDB_ROW_MAJOR;
  array_schema_c.tile_extents_ = nullptr;
  array_schema_c.dense_ = 0;

  // Set attributes
  char** attributes =
      (char**)malloc((metadata_schema_c->attribute_num_ + 1) * sizeof(char*));
  size_t attribute_len;
  for (int i = 0; i < metadata_schema_c->attribute_num_; ++i) {
    attribute_len = strlen(metadata_schema_c->attributes_[i]);
    attributes[i] = (char*)malloc(attribute_len + 1);
    strcpy(attributes[i], metadata_schema_c->attributes_[i]);
  }
  attribute_len = strlen(Configurator::key());
  attributes[metadata_schema_c->attribute_num_] =
      (char*)malloc(attribute_len + 1);
  strcpy(attributes[metadata_schema_c->attribute_num_], Configurator::key());
  array_schema_c.attributes_ = attributes;
  array_schema_c.attribute_num_ = metadata_schema_c->attribute_num_ + 1;

  // Set dimensions
  char** dimensions = (char**)malloc(4 * sizeof(char*));
  size_t dimension_len;
  dimension_len = strlen(Configurator::key_dim1_name());
  dimensions[0] = (char*)malloc(dimension_len + 1);
  strcpy(dimensions[0], Configurator::key_dim1_name());
  dimension_len = strlen(Configurator::key_dim2_name());
  dimensions[1] = (char*)malloc(dimension_len + 1);
  strcpy(dimensions[1], Configurator::key_dim2_name());
  dimension_len = strlen(Configurator::key_dim3_name());
  dimensions[2] = (char*)malloc(dimension_len + 1);
  strcpy(dimensions[2], Configurator::key_dim3_name());
  array_schema_c.dimensions_ = dimensions;
  dimension_len = strlen(Configurator::key_dim4_name());
  dimensions[3] = (char*)malloc(dimension_len + 1);
  strcpy(dimensions[3], Configurator::key_dim4_name());
  array_schema_c.dimensions_ = dimensions;
  array_schema_c.dim_num_ = 4;

  // Set domain
  int* domain = (int*)malloc(8 * sizeof(int));
  for (int i = 0; i < 4; ++i) {
    domain[2 * i] = 0;
    domain[2 * i + 1] = UINT_MAX;
  }
  array_schema_c.domain_ = domain;

  // Set types
  tiledb_datatype_t* types = (tiledb_datatype_t*)malloc(
      (metadata_schema_c->attribute_num_ + 2) * sizeof(int));
  for (int i = 0; i < metadata_schema_c->attribute_num_; ++i)
    types[i] = metadata_schema_c->types_[i];
  types[metadata_schema_c->attribute_num_] = TILEDB_CHAR;
  types[metadata_schema_c->attribute_num_ + 1] = TILEDB_UINT32;
  array_schema_c.types_ = types;

  // Set cell num val
  int* cell_val_num =
      (int*)malloc((metadata_schema_c->attribute_num_ + 1) * sizeof(int));
  if (metadata_schema_c->cell_val_num_ == nullptr) {
    for (int i = 0; i < metadata_schema_c->attribute_num_; ++i)
      cell_val_num[i] = 1;
  } else {
    for (int i = 0; i < metadata_schema_c->attribute_num_; ++i)
      cell_val_num[i] = metadata_schema_c->cell_val_num_[i];
  }
  cell_val_num[metadata_schema_c->attribute_num_] = Configurator::var_num();
  array_schema_c.cell_val_num_ = cell_val_num;

  // Set compression
  tiledb_compressor_t* compression = (tiledb_compressor_t*)malloc(
      (metadata_schema_c->attribute_num_ + 2) * sizeof(int));
  if (metadata_schema_c->compressor_ == nullptr) {
    for (int i = 0; i < metadata_schema_c->attribute_num_ + 1; ++i)
      compression[i] = TILEDB_NO_COMPRESSION;
  } else {
    for (int i = 0; i < metadata_schema_c->attribute_num_ + 1; ++i)
      compression[i] = metadata_schema_c->compressor_[i];
  }
  compression[metadata_schema_c->attribute_num_ + 1] = TILEDB_NO_COMPRESSION;
  array_schema_c.compressor_ = compression;

  // Initialize schema through the array schema C struct
  init(&array_schema_c);

  // Clean up
  for (int i = 0; i < array_schema_c.attribute_num_; ++i)
    free(attributes[i]);
  free(attributes);
  for (int i = 0; i < 4; ++i)
    free(dimensions[i]);
  free(dimensions);
  free(domain);
  free(types);
  free(compression);
  free(cell_val_num);

  // Success
  return Status::Ok();
}

Status ArraySchema::load(const std::string& dir) {
  // Get real array path
  std::string real_dir = filesystem::real_dir(dir);

  // Check if array exists
  if (!utils::is_array(real_dir))
    return LOG_STATUS(Status::ArraySchemaError(
        std::string("Cannot load array schema; Array '") + real_dir +
        "' does not exist"));

  // Open array schema file
  std::string filename = real_dir + "/" + Configurator::array_schema_filename();
  int fd = ::open(filename.c_str(), O_RDONLY);
  if (fd == -1)
    return LOG_STATUS(
        Status::ArraySchemaError("Cannot load schema; File opening error"));

  // Initialize buffer
  struct stat _stat;
  fstat(fd, &_stat);
  ssize_t buffer_size = _stat.st_size;

  if (buffer_size == 0)
    return LOG_STATUS(Status::ArraySchemaError(
        "Cannot load array schema; Empty array schema file"));
  void* buffer = malloc(buffer_size);

  // Load array schema
  ssize_t bytes_read = ::read(fd, buffer, buffer_size);
  if (bytes_read != buffer_size) {
    free(buffer);
    return LOG_STATUS(Status::ArraySchemaError(
        "Cannot load array schema; File reading error"));
  }

  // Initialize array schema
  Status st = deserialize(buffer, buffer_size);
  if (!st.ok()) {
    free(buffer);
    return st;
  }

  // Clean up
  free(buffer);
  if (::close(fd))
    return LOG_STATUS(Status::ArraySchemaError(
        "Cannot load array schema; File closing error"));

  // Success
  return Status::Ok();
}

void ArraySchema::set_array_name(const char* array_name) {
  // Get real array name
  std::string array_name_real = filesystem::real_dir(array_name);

  // Set array name
  array_name_ = array_name_real;
}

Status ArraySchema::set_attributes(char** attributes, int attribute_num) {
  // Sanity check on attributes
  if (attributes == nullptr)
    return LOG_STATUS(
        Status::ArraySchemaError("Cannot set attributes; No attributes given"));

  // Sanity check on attribute number
  if (attribute_num <= 0)
    return LOG_STATUS(
        Status::ArraySchemaError("Cannot set attributes; "
                                 "The number of attributes must be positive"));

  // Set attributes and attribute number
  for (int i = 0; i < attribute_num; ++i)
    attributes_.emplace_back(attributes[i]);
  attribute_num_ = attribute_num;

  // Append extra coordinates name
  attributes_.emplace_back(Configurator::coords());

  // Check for duplicate attribute names
  if (utils::has_duplicates(attributes_)) {
    return LOG_STATUS(Status::ArraySchemaError(
        "Cannot set attributes; Duplicate attribute names"));
  }

  // Check if a dimension has the same name as an attribute
  if (utils::intersect(attributes_, dimensions_)) {
    return LOG_STATUS(Status::ArraySchemaError(
        "Cannot set attributes; Attribute name same as dimension name"));
  }

  return Status::Ok();
}

void ArraySchema::set_capacity(int64_t capacity) {
  assert(capacity >= 0);

  // Set capacity
  if (capacity > 0)
    capacity_ = capacity;
  else
    capacity_ = Configurator::capacity();
}

void ArraySchema::set_cell_val_num(const int* cell_val_num) {
  if (cell_val_num == nullptr) {
    for (int i = 0; i < attribute_num_; ++i)
      cell_val_num_.push_back(1);
  } else {
    for (int i = 0; i < attribute_num_; ++i)
      cell_val_num_.push_back(cell_val_num[i]);
  }
}

Status ArraySchema::set_cell_order(tiledb_layout_t cell_order) {
  // Set cell order
  if (cell_order != TILEDB_ROW_MAJOR && cell_order != TILEDB_COL_MAJOR) {
    return LOG_STATUS(
        Status::ArraySchemaError("Cannot set cell order; Invalid cell order"));
  }
  cell_order_ = static_cast<Layout>(cell_order);

  // Success
  return Status::Ok();
}

Status ArraySchema::set_compression(tiledb_compressor_t* compression) {
  if (compression == nullptr) {
    for (int i = 0; i < attribute_num_ + 1; ++i)
      compressor_.push_back(Compressor::NO_COMPRESSION);
  } else {
    for (int i = 0; i < attribute_num_ + 1; ++i) {
      tiledb_compressor_t c = compression[i];
      if (c != TILEDB_NO_COMPRESSION && c != TILEDB_GZIP && c != TILEDB_ZSTD &&
          c != TILEDB_LZ4 && c != TILEDB_BLOSC && c != TILEDB_BLOSC_LZ4 &&
          c != TILEDB_BLOSC_LZ4HC && c != TILEDB_BLOSC_SNAPPY &&
          c != TILEDB_BLOSC_ZLIB && c != TILEDB_BLOSC_ZSTD && c != TILEDB_RLE &&
          c != TILEDB_BZIP2) {
        return LOG_STATUS(Status::ArraySchemaError(
            "Cannot set compression; Invalid compression type"));
      }
      compressor_.push_back(static_cast<Compressor>(c));
    }
  }
  return Status::Ok();
}

void ArraySchema::set_dense(int dense) {
  dense_ = dense;
}

Status ArraySchema::set_dimensions(char** dimensions, int dim_num) {
  // Sanity check on dimensions
  if (dimensions == nullptr) {
    return LOG_STATUS(
        Status::ArraySchemaError("Cannot set dimensions; No dimensions given"));
  }

  // Sanity check on dimension number
  if (dim_num <= 0) {
    return LOG_STATUS(
        Status::ArraySchemaError("Cannot set dimensions; "
                                 "The number of dimensions must be positive"));
  }

  // Set dimensions and dimension number
  for (int i = 0; i < dim_num; ++i)
    dimensions_.emplace_back(dimensions[i]);
  dim_num_ = dim_num;

  // Check for duplicate dimension names
  if (utils::has_duplicates(dimensions_)) {
    return LOG_STATUS(Status::ArraySchemaError(
        "Cannot set dimensions; Duplicate dimension names"));
  }

  // Check if a dimension has the same name as an attribute
  if (utils::intersect(attributes_, dimensions_)) {
    return LOG_STATUS(Status::ArraySchemaError(
        "Cannot set dimensions; Attribute name same as dimension name"));
  }

  return Status::Ok();
}

Status ArraySchema::set_domain(const void* domain) {
  // Sanity check
  if (domain == nullptr) {
    return LOG_STATUS(
        Status::ArraySchemaError("Cannot set domain; Domain not provided"));
  }

  // Clear domain
  if (domain_ != nullptr)
    free(domain_);

  // Set domain
  size_t domain_size = 2 * coords_size();
  domain_ = malloc(domain_size);
  memcpy(domain_, domain, domain_size);

  // Sanity check
  Datatype typ = types_.at(attribute_num_);
  if (typ == Datatype::INT32) {
    int* domain_int = (int*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      if (domain_int[2 * i] > domain_int[2 * i + 1]) {
        return LOG_STATUS(Status::ArraySchemaError(
            "Cannot set domain; Lower domain bound larger than its "
            "corresponding upper"));
      }
    }
  } else if (typ == Datatype::INT64) {
    int64_t* domain_int64 = (int64_t*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      if (domain_int64[2 * i] > domain_int64[2 * i + 1]) {
        return LOG_STATUS(Status::ArraySchemaError(
            "Cannot set domain; Lower domain bound larger than its "
            "corresponding upper"));
      }
    }
  } else if (typ == Datatype::FLOAT32) {
    float* domain_float = (float*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      if (domain_float[2 * i] > domain_float[2 * i + 1]) {
        return LOG_STATUS(Status::ArraySchemaError(
            "Cannot set domain; Lower domain bound larger than its "
            "corresponding upper"));
      }
    }
  } else if (typ == Datatype::FLOAT64) {
    double* domain_double = (double*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      if (domain_double[2 * i] > domain_double[2 * i + 1]) {
        return LOG_STATUS(Status::ArraySchemaError(
            "Cannot set domain; Lower domain bound larger than its "
            "corresponding upper"));
      }
    }
  } else if (typ == Datatype::INT8) {
    int8_t* domain_int8 = (int8_t*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      if (domain_int8[2 * i] > domain_int8[2 * i + 1]) {
        return LOG_STATUS(Status::ArraySchemaError(
            "Cannot set domain; Lower domain bound larger than its "
            "corresponding upper"));
      }
    }
  } else if (typ == Datatype::UINT8) {
    uint8_t* domain_uint8 = (uint8_t*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      if (domain_uint8[2 * i] > domain_uint8[2 * i + 1]) {
        return LOG_STATUS(Status::ArraySchemaError(
            "Cannot set domain; Lower domain bound larger than its "
            "corresponding upper"));
      }
    }
  } else if (typ == Datatype::INT16) {
    int16_t* domain_int16 = (int16_t*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      if (domain_int16[2 * i] > domain_int16[2 * i + 1]) {
        return LOG_STATUS(Status::ArraySchemaError(
            "Cannot set domain; Lower domain bound larger than its "
            "corresponding upper"));
      }
    }
  } else if (typ == Datatype::UINT16) {
    uint16_t* domain_uint16 = (uint16_t*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      if (domain_uint16[2 * i] > domain_uint16[2 * i + 1]) {
        return LOG_STATUS(Status::ArraySchemaError(
            "Cannot set domain; Lower domain bound larger than its "
            "corresponding upper"));
      }
    }
  } else if (typ == Datatype::UINT32) {
    uint32_t* domain_uint32 = (uint32_t*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      if (domain_uint32[2 * i] > domain_uint32[2 * i + 1]) {
        return LOG_STATUS(Status::ArraySchemaError(
            "Cannot set domain; Lower domain bound larger than its "
            "corresponding upper"));
      }
    }
  } else if (typ == Datatype::UINT64) {
    uint64_t* domain_uint64 = (uint64_t*)domain_;
    for (int i = 0; i < dim_num_; ++i) {
      if (domain_uint64[2 * i] > domain_uint64[2 * i + 1]) {
        return LOG_STATUS(Status::ArraySchemaError(
            "Cannot set domain; Lower domain bound larger than its "
            "corresponding upper"));
      }
    }
  } else {
    return LOG_STATUS(Status::ArraySchemaError(
        "Cannot set domain; Invalid coordinates type"));
  }

  return Status::Ok();
}

Status ArraySchema::set_tile_extents(const void* tile_extents) {
  // Dense arrays must have tile extents
  if (tile_extents == nullptr && dense_) {
    return LOG_STATUS(Status::ArraySchemaError(
        "Cannot set tile extents; Dense arrays must have tile extents"));
  }

  // Free existing tile extends
  if (tile_extents_ != nullptr)
    free(tile_extents_);

  // Set tile extents
  if (tile_extents == nullptr) {
    tile_extents_ = nullptr;
  } else {
    size_t tile_extents_size = coords_size();
    tile_extents_ = malloc(tile_extents_size);
    memcpy(tile_extents_, tile_extents, tile_extents_size);
  }
  return Status::Ok();
}

Status ArraySchema::set_tile_order(tiledb_layout_t tile_order) {
  // Set tile order
  if (tile_order != TILEDB_ROW_MAJOR && tile_order != TILEDB_COL_MAJOR) {
    return LOG_STATUS(
        Status::ArraySchemaError("Cannot set tile order; Invalid tile order"));
  }
  tile_order_ = static_cast<Layout>(tile_order);

  // Success
  return Status::Ok();
}

Status ArraySchema::set_types(const tiledb_datatype_t* types) {
  // Sanity check
  if (types == nullptr) {
    return LOG_STATUS(
        Status::ArraySchemaError("Cannot set types; Types not provided"));
  }

  // Set attribute types
  tiledb_datatype_t typ;
  for (int i = 0; i < attribute_num_; ++i) {
    typ = types[i];
    if (typ != TILEDB_INT32 && typ != TILEDB_INT64 && typ != TILEDB_FLOAT32 &&
        typ != TILEDB_FLOAT64 && typ != TILEDB_INT8 && typ != TILEDB_UINT8 &&
        typ != TILEDB_INT16 && types[i] != TILEDB_UINT16 &&
        typ != TILEDB_UINT32 && types[i] != TILEDB_UINT64 &&
        typ != TILEDB_CHAR) {
      return LOG_STATUS(
          Status::ArraySchemaError("Cannot set types; Invalid type"));
    }
    types_.push_back(static_cast<Datatype>(typ));
  }

  // Set coordinate type
  typ = types[attribute_num_];
  if (typ != TILEDB_INT32 && typ != TILEDB_INT64 && typ != TILEDB_FLOAT32 &&
      typ != TILEDB_FLOAT64 && typ != TILEDB_INT8 && typ != TILEDB_UINT8 &&
      typ != TILEDB_INT16 && typ != TILEDB_UINT16 && typ != TILEDB_UINT32 &&
      typ != TILEDB_UINT64) {
    return LOG_STATUS(
        Status::ArraySchemaError("Cannot set types; Invalid type"));
  }
  types_.push_back(static_cast<Datatype>(typ));

  // Set type sizes
  type_sizes_.resize(attribute_num_ + 1);
  for (int i = 0; i < attribute_num_ + 1; ++i)
    type_sizes_[i] = compute_type_size(i);

  // Set cell sizes
  cell_sizes_.resize(attribute_num_ + 1);
  for (int i = 0; i < attribute_num_ + 1; ++i)
    cell_sizes_[i] = compute_cell_size(i);

  // Set the coordinates size
  coords_size_ = cell_sizes_[attribute_num_];

  return Status::Ok();
}

/* ****************************** */
/*              MISC              */
/* ****************************** */

template <class T>
int ArraySchema::cell_order_cmp(const T* coords_a, const T* coords_b) const {
  // Check if they are equal
  if (memcmp(coords_a, coords_b, coords_size_) == 0)
    return 0;

  // Check for precedence
  if (cell_order_ == Layout::COL_MAJOR) {  // COLUMN-MAJOR
    for (int i = dim_num_ - 1; i >= 0; --i) {
      if (coords_a[i] < coords_b[i])
        return -1;
      else if (coords_a[i] > coords_b[i])
        return 1;
    }
  } else if (cell_order_ == Layout::ROW_MAJOR) {  // ROW-MAJOR
    for (int i = 0; i < dim_num_; ++i) {
      if (coords_a[i] < coords_b[i])
        return -1;
      else if (coords_a[i] > coords_b[i])
        return 1;
    }
  } else {  // Invalid cell order
    assert(0);
  }

  // The program should never reach this point
  assert(0);
  return 0;
}

void ArraySchema::expand_domain(void* domain) const {
  Datatype typ = types_.at(attribute_num_);
  switch (typ) {
    case Datatype::INT32:
      expand_domain<int>(static_cast<int*>(domain));
      break;
    case Datatype::INT64:
      expand_domain<int64_t>(static_cast<int64_t*>(domain));
      break;
    case Datatype::INT8:
      expand_domain<int8_t>(static_cast<int8_t*>(domain));
      break;
    case Datatype::UINT8:
      expand_domain<uint8_t>(static_cast<uint8_t*>(domain));
      break;
    case Datatype::INT16:
      expand_domain<int16_t>(static_cast<int16_t*>(domain));
      break;
    case Datatype::UINT16:
      expand_domain<uint16_t>(static_cast<uint16_t*>(domain));
      break;
    case Datatype::UINT32:
      expand_domain<uint32_t>(static_cast<uint32_t*>(domain));
      break;
    case Datatype::UINT64:
      expand_domain<uint64_t>(static_cast<uint64_t*>(domain));
      break;
    default:
      assert(0);
  }
}

template <class T>
void ArraySchema::expand_domain(T* domain) const {
  // Applicable only to regular tiles
  if (tile_extents_ == nullptr)
    return;

  const T* tile_extents = static_cast<const T*>(tile_extents_);
  const T* array_domain = static_cast<const T*>(domain_);

  for (int i = 0; i < dim_num_; ++i) {
    domain[2 * i] = ((domain[2 * i] - array_domain[2 * i]) / tile_extents[i] *
                     tile_extents[i]) +
                    array_domain[2 * i];
    domain[2 * i + 1] =
        ((domain[2 * i + 1] - array_domain[2 * i]) / tile_extents[i] + 1) *
            tile_extents[i] -
        1 + array_domain[2 * i];
  }
}

template <class T>
Status ArraySchema::get_cell_pos(const T* coords, int64_t* pos) const {
  // Applicable only to dense arrays
  if (!dense_) {
    return LOG_STATUS(Status::ArraySchemaError(
        "Cannot get cell position; Invalid array type"));
  }

  // Invoke the proper function based on the cell order
  if (cell_order_ == Layout::ROW_MAJOR) {
    *pos = get_cell_pos_row(coords);
    return Status::Ok();
  } else if (cell_order_ == Layout::COL_MAJOR) {
    *pos = get_cell_pos_col(coords);
    return Status::Ok();
  }
  return LOG_STATUS(
      Status::ArraySchemaError("Cannot get cell position; Invalid cell order"));
}

template <class T>
void ArraySchema::get_next_cell_coords(
    const T* domain, T* cell_coords, bool& coords_retrieved) const {
  // Sanity check
  assert(dense_);

  // Invoke the proper function based on the tile order
  if (cell_order_ == Layout::ROW_MAJOR)
    get_next_cell_coords_row(domain, cell_coords, coords_retrieved);
  else if (cell_order_ == Layout::COL_MAJOR)
    get_next_cell_coords_col(domain, cell_coords, coords_retrieved);
  else  // Sanity check
    assert(0);
}

template <class T>
void ArraySchema::get_next_tile_coords(const T* domain, T* tile_coords) const {
  // Sanity check
  assert(dense_);

  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR)
    get_next_tile_coords_row(domain, tile_coords);
  else if (tile_order_ == Layout::COL_MAJOR)
    get_next_tile_coords_col(domain, tile_coords);
  else  // Sanity check
    assert(0);
}

template <class T>
void ArraySchema::get_previous_cell_coords(
    const T* domain, T* cell_coords) const {
  // Sanity check
  assert(dense_);

  // Invoke the proper function based on the tile order
  if (cell_order_ == Layout::ROW_MAJOR)
    get_previous_cell_coords_row(domain, cell_coords);
  else if (cell_order_ == Layout::COL_MAJOR)
    get_previous_cell_coords_col(domain, cell_coords);
  else  // Sanity check
    assert(0);
}

template <class T>
void ArraySchema::get_subarray_tile_domain(
    const T* subarray, T* tile_domain, T* subarray_tile_domain) const {
  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  // Get tile domain
  T tile_num;  // Per dimension
  for (int i = 0; i < dim_num_; ++i) {
    tile_num =
        ceil(double(domain[2 * i + 1] - domain[2 * i] + 1) / tile_extents[i]);
    tile_domain[2 * i] = 0;
    tile_domain[2 * i + 1] = tile_num - 1;
  }

  // Calculate subarray in tile domain
  for (int i = 0; i < dim_num_; ++i) {
    subarray_tile_domain[2 * i] =
        MAX((subarray[2 * i] - domain[2 * i]) / tile_extents[i],
            tile_domain[2 * i]);
    subarray_tile_domain[2 * i + 1] =
        MIN((subarray[2 * i + 1] - domain[2 * i]) / tile_extents[i],
            tile_domain[2 * i + 1]);
  }
}

template <class T>
int64_t ArraySchema::get_tile_pos(const T* tile_coords) const {
  // Sanity check
  assert(tile_extents_);

  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR) {
    return get_tile_pos_row(tile_coords);
  } else if (tile_order_ == Layout::COL_MAJOR) {
    return get_tile_pos_col(tile_coords);
  } else {  // Sanity check
    assert(0);
  }

  // Code should never reach here
  return -1;
}

template <class T>
int64_t ArraySchema::get_tile_pos(const T* domain, const T* tile_coords) const {
  // Sanity check
  assert(tile_extents_);

  // Invoke the proper function based on the tile order
  if (tile_order_ == Layout::ROW_MAJOR) {
    return get_tile_pos_row(domain, tile_coords);
  } else if (tile_order_ == Layout::COL_MAJOR) {
    return get_tile_pos_col(domain, tile_coords);
  } else {  // Sanity check
    assert(0);
  }

  // Code should never reach here
  return -1;
}

template <class T>
void ArraySchema::get_tile_subarray(
    const T* tile_coords, T* tile_subarray) const {
  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  for (int i = 0; i < dim_num_; ++i) {
    tile_subarray[2 * i] = tile_coords[i] * tile_extents[i] + domain[2 * i];
    tile_subarray[2 * i + 1] =
        (tile_coords[i] + 1) * tile_extents[i] - 1 + domain[2 * i];
  }
}

template <class T>
int ArraySchema::tile_cell_order_cmp(
    const T* coords_a, const T* coords_b) const {
  // Check tile order
  int tile_cmp = tile_order_cmp(coords_a, coords_b);
  if (tile_cmp)
    return tile_cmp;

  // Check cell order
  return cell_order_cmp(coords_a, coords_b);
}

template <typename T>
inline int64_t ArraySchema::tile_id(const T* cell_coords) const {
  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  // Trivial case
  if (tile_extents == nullptr)
    return 0;

  // Calculate tile coordinates
  T* tile_coords = static_cast<T*>(tile_coords_aux_);
  for (int i = 0; i < dim_num_; ++i)
    tile_coords[i] = (cell_coords[i] - domain[2 * i]) / tile_extents[i];

  int64_t tile_id = get_tile_pos(tile_coords);

  // Return
  return tile_id;
}

template <class T>
int ArraySchema::tile_order_cmp(const T* coords_a, const T* coords_b) const {
  // Calculate tile ids
  int64_t id_a = tile_id(coords_a);
  int64_t id_b = tile_id(coords_b);

  // Compare ids
  if (id_a < id_b)
    return -1;
  else if (id_a > id_b)
    return 1;
  else  // id_a == id_b
    return 0;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

void ArraySchema::clear() {
  array_name_ = "";
  attributes_.clear();
  capacity_ = -1;
  dimensions_.clear();
  compressor_.clear();
  compression_level_.clear();
  dense_ = -1;
  cell_val_num_.clear();
  types_.clear();
  if (tile_extents_ != nullptr) {
    free(tile_extents_);
    tile_extents_ = nullptr;
  }
  if (domain_ != nullptr) {
    free(domain_);
    domain_ = nullptr;
  }
}

// ===== FORMAT =====
// array_name_size(int)
//     array_name(string)
// dense(bool)
// tile_order(char)
// cell_order(char)
// capacity(int64_t)
// attribute_num(int)
//     attribute_size#1(int) attribute#1(string)
//     attribute_size#2(int) attribute#2(string)
//     ...
// dim_num(int)
//    dimension_size#1(int) dimension#1(string)
//    dimension_size#2(int) dimension#2(string)
//    ...
// domain_size(int)
// domain#1_low(coordinates type) dim_domain#1_high(coordinates type)
// domain#2_low(coordinates type) dim_domain#2_high(coordinates type)
//  ...
// tile_extents_size(int)
//     tile_extent#1(coordinates type) tile_extent#2(coordinates type) ...
// type#1(char) type#2(char) ...
// cell_val_num#1(int) cell_val_num#2(int) ...
// compression#1(char) compression#2(char) ...
size_t ArraySchema::compute_bin_size() const {
  // Initialization
  size_t bin_size = 0;

  // Size for array_name_
  bin_size += sizeof(int) + array_name_.size();
  // Size for dense_
  bin_size += sizeof(bool);
  // Size for tile_order_ and cell_order_
  bin_size += 2 * sizeof(char);
  // Size for capacity_
  bin_size += sizeof(int64_t);
  // Size for attribute_num_ and attributes_
  bin_size += sizeof(int);
  for (int i = 0; i < attribute_num_; ++i)
    bin_size += sizeof(int) + attributes_[i].size();
  // Size for dim_num and dimensions_
  bin_size += sizeof(int);
  for (int i = 0; i < dim_num_; ++i)
    bin_size += sizeof(int) + dimensions_[i].size();
  // Size for domain_
  bin_size += sizeof(int) + 2 * coords_size();
  // Size for tile_extents_
  bin_size += sizeof(int) + ((tile_extents_ == nullptr) ? 0 : coords_size());
  // Size for types_
  bin_size += (attribute_num_ + 1) * sizeof(char);
  // Size for cell_val_num_
  bin_size += attribute_num_ * sizeof(int);
  // Size for compressor_
  bin_size += (attribute_num_ + 1) * sizeof(char);

  return bin_size;
}

void ArraySchema::compute_cell_num_per_tile() {
  //  Meaningful only for dense arrays
  if (!dense_)
    return;

  // Invoke the proper templated function
  Datatype coords_type = types_.at(attribute_num_);
  if (coords_type == Datatype::INT32)
    compute_cell_num_per_tile<int>();
  else if (coords_type == Datatype::INT64)
    compute_cell_num_per_tile<int64_t>();
  else if (coords_type == Datatype::INT8)
    compute_cell_num_per_tile<int8_t>();
  else if (coords_type == Datatype::UINT8)
    compute_cell_num_per_tile<uint8_t>();
  else if (coords_type == Datatype::INT16)
    compute_cell_num_per_tile<int16_t>();
  else if (coords_type == Datatype::UINT16)
    compute_cell_num_per_tile<uint16_t>();
  else if (coords_type == Datatype::UINT32)
    compute_cell_num_per_tile<uint32_t>();
  else if (coords_type == Datatype::UINT64)
    compute_cell_num_per_tile<uint64_t>();
  else  // Sanity check
    assert(0);
}

template <class T>
void ArraySchema::compute_cell_num_per_tile() {
  const T* tile_extents = static_cast<const T*>(tile_extents_);
  cell_num_per_tile_ = 1;

  for (int i = 0; i < dim_num_; ++i)
    cell_num_per_tile_ *= tile_extents[i];
}

size_t ArraySchema::compute_cell_size(int i) const {
  assert(i >= 0 && i <= attribute_num_);

  // Variable-sized cell
  if (i < attribute_num_ && cell_val_num_[i] == Configurator::var_num())
    return Configurator::var_size();

  // Fixed-sized cell
  size_t size = 0;

  // Attributes
  if (i < attribute_num_) {
    if (types_[i] == Datatype::CHAR)
      size = cell_val_num_[i] * sizeof(char);
    else if (types_[i] == Datatype::INT32)
      size = cell_val_num_[i] * sizeof(int);
    else if (types_[i] == Datatype::INT64)
      size = cell_val_num_[i] * sizeof(int64_t);
    else if (types_[i] == Datatype::FLOAT32)
      size = cell_val_num_[i] * sizeof(float);
    else if (types_[i] == Datatype::FLOAT64)
      size = cell_val_num_[i] * sizeof(double);
    else if (types_[i] == Datatype::INT8)
      size = cell_val_num_[i] * sizeof(int8_t);
    else if (types_[i] == Datatype::UINT8)
      size = cell_val_num_[i] * sizeof(uint8_t);
    else if (types_[i] == Datatype::INT16)
      size = cell_val_num_[i] * sizeof(int16_t);
    else if (types_[i] == Datatype::UINT16)
      size = cell_val_num_[i] * sizeof(uint16_t);
    else if (types_[i] == Datatype::UINT32)
      size = cell_val_num_[i] * sizeof(uint32_t);
    else if (types_[i] == Datatype::UINT64)
      size = cell_val_num_[i] * sizeof(uint64_t);
  } else {  // Coordinates
    if (types_[i] == Datatype::INT32)
      size = dim_num_ * sizeof(int);
    else if (types_[i] == Datatype::INT64)
      size = dim_num_ * sizeof(int64_t);
    else if (types_[i] == Datatype::FLOAT32)
      size = dim_num_ * sizeof(float);
    else if (types_[i] == Datatype::FLOAT64)
      size = dim_num_ * sizeof(double);
    else if (types_[i] == Datatype::INT8)
      size = dim_num_ * sizeof(int8_t);
    else if (types_[i] == Datatype::UINT8)
      size = dim_num_ * sizeof(uint8_t);
    else if (types_[i] == Datatype::INT16)
      size = dim_num_ * sizeof(int16_t);
    else if (types_[i] == Datatype::UINT16)
      size = dim_num_ * sizeof(uint16_t);
    else if (types_[i] == Datatype::UINT32)
      size = dim_num_ * sizeof(uint32_t);
    else if (types_[i] == Datatype::UINT64)
      size = dim_num_ * sizeof(uint64_t);
  }

  return size;
}

void ArraySchema::compute_tile_domain() {
  // For easy reference
  Datatype coords_type = types_[attribute_num_];

  // Invoke the proper templated function
  if (coords_type == Datatype::INT32)
    compute_tile_domain<int>();
  else if (coords_type == Datatype::INT64)
    compute_tile_domain<int64_t>();
  else if (coords_type == Datatype::FLOAT32)
    compute_tile_domain<float>();
  else if (coords_type == Datatype::FLOAT64)
    compute_tile_domain<double>();
  else if (coords_type == Datatype::INT8)
    compute_tile_domain<int8_t>();
  else if (coords_type == Datatype::UINT8)
    compute_tile_domain<uint8_t>();
  else if (coords_type == Datatype::INT16)
    compute_tile_domain<int16_t>();
  else if (coords_type == Datatype::UINT16)
    compute_tile_domain<uint16_t>();
  else if (coords_type == Datatype::UINT32)
    compute_tile_domain<uint32_t>();
  else if (coords_type == Datatype::UINT64)
    compute_tile_domain<uint64_t>();
  else
    assert(0);
}

template <class T>
void ArraySchema::compute_tile_domain() {
  if (tile_extents_ == nullptr)
    return;

  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  // Allocate space for the tile domain
  assert(tile_domain_ == NULL);
  tile_domain_ = malloc(2 * dim_num_ * sizeof(T));

  // For easy reference
  T* tile_domain = static_cast<T*>(tile_domain_);
  T tile_num;  // Per dimension

  // Calculate tile domain
  for (int i = 0; i < dim_num_; ++i) {
    tile_num =
        ceil(double(domain[2 * i + 1] - domain[2 * i] + 1) / tile_extents[i]);
    tile_domain[2 * i] = 0;
    tile_domain[2 * i + 1] = tile_num - 1;
  }
}

void ArraySchema::compute_tile_offsets() {
  // Invoke the proper templated function
  if (types_[attribute_num_] == Datatype::INT32) {
    compute_tile_offsets<int>();
  } else if (types_[attribute_num_] == Datatype::INT64) {
    compute_tile_offsets<int64_t>();
  } else if (types_[attribute_num_] == Datatype::FLOAT32) {
    compute_tile_offsets<float>();
  } else if (types_[attribute_num_] == Datatype::FLOAT64) {
    compute_tile_offsets<double>();
  } else if (types_[attribute_num_] == Datatype::INT8) {
    compute_tile_offsets<int8_t>();
  } else if (types_[attribute_num_] == Datatype::UINT8) {
    compute_tile_offsets<uint8_t>();
  } else if (types_[attribute_num_] == Datatype::INT16) {
    compute_tile_offsets<int16_t>();
  } else if (types_[attribute_num_] == Datatype::UINT16) {
    compute_tile_offsets<uint16_t>();
  } else if (types_[attribute_num_] == Datatype::UINT32) {
    compute_tile_offsets<uint32_t>();
  } else if (types_[attribute_num_] == Datatype::UINT64) {
    compute_tile_offsets<uint64_t>();
  } else {  // The program should never reach this point
    assert(0);
  }
}

template <class T>
void ArraySchema::compute_tile_offsets() {
  // Applicable only to non-NULL space tiles
  if (tile_extents_ == nullptr)
    return;

  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);
  int64_t tile_num;  // Per dimension

  // Calculate tile offsets for column-major tile order
  tile_offsets_col_.push_back(1);
  for (int i = 1; i < dim_num_; ++i) {
    tile_num = (domain[2 * (i - 1) + 1] - domain[2 * (i - 1)] + 1) /
               tile_extents[i - 1];
    tile_offsets_col_.push_back(tile_offsets_col_.back() * tile_num);
  }

  // Calculate tile offsets for row-major tile order
  tile_offsets_row_.push_back(1);
  for (int i = dim_num_ - 2; i >= 0; --i) {
    tile_num = (domain[2 * (i + 1) + 1] - domain[2 * (i + 1)] + 1) /
               tile_extents[i + 1];
    tile_offsets_row_.push_back(tile_offsets_row_.back() * tile_num);
  }
  std::reverse(tile_offsets_row_.begin(), tile_offsets_row_.end());
}

size_t ArraySchema::compute_type_size(int i) const {
  // Sanity check
  assert(i >= 0 && i <= attribute_num_);

  if (types_[i] == Datatype::CHAR) {
    return sizeof(char);
  } else if (types_[i] == Datatype::INT32) {
    return sizeof(int);
  } else if (types_[i] == Datatype::INT64) {
    return sizeof(int64_t);
  } else if (types_[i] == Datatype::FLOAT32) {
    return sizeof(float);
  } else if (types_[i] == Datatype::FLOAT64) {
    return sizeof(double);
  } else if (types_[i] == Datatype::INT8) {
    return sizeof(int8_t);
  } else if (types_[i] == Datatype::UINT8) {
    return sizeof(uint8_t);
  } else if (types_[i] == Datatype::INT16) {
    return sizeof(int16_t);
  } else if (types_[i] == Datatype::UINT16) {
    return sizeof(uint16_t);
  } else if (types_[i] == Datatype::UINT32) {
    return sizeof(uint32_t);
  } else if (types_[i] == Datatype::UINT64) {
    return sizeof(uint64_t);
  } else {  // The program should never reach this point
    assert(0);
    return 0;
  }
}

template <class T>
int64_t ArraySchema::get_cell_pos_col(const T* coords) const {
  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate cell offsets
  int64_t cell_num;  // Per dimension
  std::vector<int64_t> cell_offsets;
  cell_offsets.push_back(1);
  for (int i = 1; i < dim_num_; ++i) {
    cell_num = tile_extents[i - 1];
    cell_offsets.push_back(cell_offsets.back() * cell_num);
  }

  // Calculate position
  T coords_norm;  // Normalized coordinates inside the tile
  int64_t pos = 0;
  for (int i = 0; i < dim_num_; ++i) {
    coords_norm = (coords[i] - domain[2 * i]);
    coords_norm -= (coords_norm / tile_extents[i]) * tile_extents[i];
    pos += coords_norm * cell_offsets[i];
  }

  // Return
  return pos;
}

template <class T>
int64_t ArraySchema::get_cell_pos_row(const T* coords) const {
  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate cell offsets
  int64_t cell_num;  // Per dimension
  std::vector<int64_t> cell_offsets;
  cell_offsets.push_back(1);
  for (int i = dim_num_ - 2; i >= 0; --i) {
    cell_num = tile_extents[i + 1];
    cell_offsets.push_back(cell_offsets.back() * cell_num);
  }
  std::reverse(cell_offsets.begin(), cell_offsets.end());

  // Calculate position
  T coords_norm;  // Normalized coordinates inside the tile
  int64_t pos = 0;
  for (int i = 0; i < dim_num_; ++i) {
    coords_norm = (coords[i] - domain[2 * i]);
    coords_norm -= (coords_norm / tile_extents[i]) * tile_extents[i];
    pos += coords_norm * cell_offsets[i];
  }

  // Return
  return pos;
}

template <class T>
void ArraySchema::get_next_cell_coords_col(
    const T* domain, T* cell_coords, bool& coords_retrieved) const {
  int i = 0;
  ++cell_coords[i];

  while (i < dim_num_ - 1 && cell_coords[i] > domain[2 * i + 1]) {
    cell_coords[i] = domain[2 * i];
    ++cell_coords[++i];
  }

  if (i == dim_num_ - 1 && cell_coords[i] > domain[2 * i + 1])
    coords_retrieved = false;
  else
    coords_retrieved = true;
}

template <class T>
void ArraySchema::get_next_cell_coords_row(
    const T* domain, T* cell_coords, bool& coords_retrieved) const {
  int i = dim_num_ - 1;
  ++cell_coords[i];

  while (i > 0 && cell_coords[i] > domain[2 * i + 1]) {
    cell_coords[i] = domain[2 * i];
    ++cell_coords[--i];
  }

  if (i == 0 && cell_coords[i] > domain[2 * i + 1])
    coords_retrieved = false;
  else
    coords_retrieved = true;
}

template <class T>
void ArraySchema::get_previous_cell_coords_col(
    const T* domain, T* cell_coords) const {
  int i = 0;
  --cell_coords[i];

  while (i < dim_num_ - 1 && cell_coords[i] < domain[2 * i]) {
    cell_coords[i] = domain[2 * i + 1];
    --cell_coords[++i];
  }
}

template <class T>
void ArraySchema::get_previous_cell_coords_row(
    const T* domain, T* cell_coords) const {
  int i = dim_num_ - 1;
  --cell_coords[i];

  while (i > 0 && cell_coords[i] < domain[2 * i]) {
    cell_coords[i] = domain[2 * i + 1];
    --cell_coords[--i];
  }
}

template <class T>
void ArraySchema::get_next_tile_coords_col(
    const T* domain, T* tile_coords) const {
  int i = 0;
  ++tile_coords[i];

  while (i < dim_num_ - 1 && tile_coords[i] > domain[2 * i + 1]) {
    tile_coords[i] = domain[2 * i];
    ++tile_coords[++i];
  }
}

template <class T>
void ArraySchema::get_next_tile_coords_row(
    const T* domain, T* tile_coords) const {
  int i = dim_num_ - 1;
  ++tile_coords[i];

  while (i > 0 && tile_coords[i] > domain[2 * i + 1]) {
    tile_coords[i] = domain[2 * i];
    ++tile_coords[--i];
  }
}

template <class T>
int64_t ArraySchema::get_tile_pos_col(const T* tile_coords) const {
  // Calculate position
  int64_t pos = 0;
  for (int i = 0; i < dim_num_; ++i)
    pos += tile_coords[i] * tile_offsets_col_[i];

  // Return
  return pos;
}

template <class T>
int64_t ArraySchema::get_tile_pos_col(
    const T* domain, const T* tile_coords) const {
  // For easy reference
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate tile offsets
  int64_t tile_num;  // Per dimension
  std::vector<int64_t> tile_offsets;
  tile_offsets.push_back(1);
  for (int i = 1; i < dim_num_; ++i) {
    tile_num = (domain[2 * (i - 1) + 1] - domain[2 * (i - 1)] + 1) /
               tile_extents[i - 1];
    tile_offsets.push_back(tile_offsets.back() * tile_num);
  }

  // Calculate position
  int64_t pos = 0;
  for (int i = 0; i < dim_num_; ++i)
    pos += tile_coords[i] * tile_offsets[i];

  // Return
  return pos;
}

template <class T>
int64_t ArraySchema::get_tile_pos_row(const T* tile_coords) const {
  // Calculate position
  int64_t pos = 0;
  for (int i = 0; i < dim_num_; ++i)
    pos += tile_coords[i] * tile_offsets_row_[i];

  // Return
  return pos;
}

template <class T>
int64_t ArraySchema::get_tile_pos_row(
    const T* domain, const T* tile_coords) const {
  // For easy reference
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  // Calculate tile offsets
  int64_t tile_num;  // Per dimension
  std::vector<int64_t> tile_offsets;
  tile_offsets.push_back(1);
  for (int i = dim_num_ - 2; i >= 0; --i) {
    tile_num = (domain[2 * (i + 1) + 1] - domain[2 * (i + 1)] + 1) /
               tile_extents[i + 1];
    tile_offsets.push_back(tile_offsets.back() * tile_num);
  }
  std::reverse(tile_offsets.begin(), tile_offsets.end());

  // Calculate position
  int64_t pos = 0;
  for (int i = 0; i < dim_num_; ++i)
    pos += tile_coords[i] * tile_offsets[i];

  // Return
  return pos;
}

template <class T>
bool ArraySchema::is_contained_in_tile_slab_col(const T* range) const {
  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);
  int64_t tile_l, tile_h;

  // Check if range is not contained in a column tile slab
  for (int i = 1; i < dim_num_; ++i) {
    tile_l = floor((range[2 * i] - domain[2 * i]) / tile_extents[i]);
    tile_h = floor((range[2 * i + 1] - domain[2 * i]) / tile_extents[i]);
    if (tile_l != tile_h)
      return false;
  }

  // Range contained in the column tile slab
  return true;
}

template <class T>
bool ArraySchema::is_contained_in_tile_slab_row(const T* range) const {
  // For easy reference
  const T* domain = static_cast<const T*>(domain_);
  const T* tile_extents = static_cast<const T*>(tile_extents_);
  int64_t tile_l, tile_h;

  // Check if range is not contained in a row tile slab
  for (int i = 0; i < dim_num_ - 1; ++i) {
    tile_l = floor((range[2 * i] - domain[2 * i]) / tile_extents[i]);
    tile_h = floor((range[2 * i + 1] - domain[2 * i]) / tile_extents[i]);
    if (tile_l != tile_h)
      return false;
  }

  // Range contained in the row tile slab
  return true;
}

void ArraySchema::set_default() {
  // Clear all members
  clear();

  // Set array name
  array_name_ = "";
  // Set attribute
  attributes_.emplace_back("a");
  attribute_num_ = 1;
  // Set capacity
  capacity_ = INT_MAX;
  // Set dimension
  dimensions_.emplace_back("d");
  dim_num_ = 1;
  // Set compression
  compressor_.emplace_back(Compressor::NO_COMPRESSION);
  compressor_.emplace_back(Compressor::NO_COMPRESSION);
  // Set compression level
  compression_level_.emplace_back(0);
  compression_level_.emplace_back(0);
  // Set dense
  dense_ = 0;
  // Set number of values per cell
  cell_val_num_.emplace_back(Configurator::var_num());
  // Set types
  types_.emplace_back(Datatype::CHAR);
  types_.emplace_back(Datatype::INT32);
  // Set tile extents
  tile_extents_ = malloc(sizeof(int));
  int* tile_extents = (int*)tile_extents_;
  *tile_extents = INT_MAX;
  // Set cell order
  cell_order_ = Layout::ROW_MAJOR;
  // Set tile order
  tile_order_ = Layout::ROW_MAJOR;
  // Set domain
  domain_ = malloc(2 * sizeof(int));
  int* domain = (int*)domain_;
  domain[0] = 0;
  domain[1] = INT_MAX;
}

template <class T>
int64_t ArraySchema::tile_slab_col_cell_num(const T* subarray) const {
  // For easy reference
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  // Initialize the cell num to be returned to the maximum number of rows
  // in the slab
  int64_t cell_num =
      MIN(tile_extents[dim_num_ - 1],
          subarray[2 * (dim_num_ - 1) + 1] - subarray[2 * (dim_num_ - 1)] + 1);

  // Calculate the number of cells in the slab
  for (int i = 0; i < dim_num_ - 1; ++i)
    cell_num *= (subarray[2 * i + 1] - subarray[2 * i] + 1);

  // Return
  return cell_num;
}

template <class T>
int64_t ArraySchema::tile_slab_row_cell_num(const T* subarray) const {
  // For easy reference
  const T* tile_extents = static_cast<const T*>(tile_extents_);

  // Initialize the cell num to be returned to the maximum number of rows
  // in the slab
  int64_t cell_num = MIN(tile_extents[0], subarray[1] - subarray[0] + 1);

  // Calculate the number of cells in the slab
  for (int i = 1; i < dim_num_; ++i)
    cell_num *= (subarray[2 * i + 1] - subarray[2 * i] + 1);

  // Return
  return cell_num;
}

// Explicit template instantiations

template int ArraySchema::cell_order_cmp<int>(
    const int* coords_a, const int* coords_b) const;
template int ArraySchema::cell_order_cmp<int64_t>(
    const int64_t* coords_a, const int64_t* coords_b) const;
template int ArraySchema::cell_order_cmp<float>(
    const float* coords_a, const float* coords_b) const;
template int ArraySchema::cell_order_cmp<double>(
    const double* coords_a, const double* coords_b) const;
template int ArraySchema::cell_order_cmp<int8_t>(
    const int8_t* coords_a, const int8_t* coords_b) const;
template int ArraySchema::cell_order_cmp<uint8_t>(
    const uint8_t* coords_a, const uint8_t* coords_b) const;
template int ArraySchema::cell_order_cmp<int16_t>(
    const int16_t* coords_a, const int16_t* coords_b) const;
template int ArraySchema::cell_order_cmp<uint16_t>(
    const uint16_t* coords_a, const uint16_t* coords_b) const;
template int ArraySchema::cell_order_cmp<uint32_t>(
    const uint32_t* coords_a, const uint32_t* coords_b) const;
template int ArraySchema::cell_order_cmp<uint64_t>(
    const uint64_t* coords_a, const uint64_t* coords_b) const;

template Status ArraySchema::get_cell_pos<int>(
    const int* coords, int64_t* pos) const;
template Status ArraySchema::get_cell_pos<int64_t>(
    const int64_t* coords, int64_t* pos) const;
template Status ArraySchema::get_cell_pos<float>(
    const float* coords, int64_t* pos) const;
template Status ArraySchema::get_cell_pos<double>(
    const double* coords, int64_t* pos) const;
template Status ArraySchema::get_cell_pos<int8_t>(
    const int8_t* coords, int64_t* pos) const;
template Status ArraySchema::get_cell_pos<uint8_t>(
    const uint8_t* coords, int64_t* pos) const;
template Status ArraySchema::get_cell_pos<int16_t>(
    const int16_t* coords, int64_t* pos) const;
template Status ArraySchema::get_cell_pos<uint16_t>(
    const uint16_t* coords, int64_t* pos) const;
template Status ArraySchema::get_cell_pos<uint32_t>(
    const uint32_t* coords, int64_t* pos) const;
template Status ArraySchema::get_cell_pos<uint64_t>(
    const uint64_t* coords, int64_t* pos) const;

template void ArraySchema::get_next_cell_coords<int>(
    const int* domain, int* cell_coords, bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<int64_t>(
    const int64_t* domain, int64_t* cell_coords, bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<float>(
    const float* domain, float* cell_coords, bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<double>(
    const double* domain, double* cell_coords, bool& coords_retrieved) const;

template void ArraySchema::get_next_cell_coords<int8_t>(
    const int8_t* domain, int8_t* cell_coords, bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<uint8_t>(
    const uint8_t* domain, uint8_t* cell_coords, bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<int16_t>(
    const int16_t* domain, int16_t* cell_coords, bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<uint16_t>(
    const uint16_t* domain,
    uint16_t* cell_coords,
    bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<uint32_t>(
    const uint32_t* domain,
    uint32_t* cell_coords,
    bool& coords_retrieved) const;
template void ArraySchema::get_next_cell_coords<uint64_t>(
    const uint64_t* domain,
    uint64_t* cell_coords,
    bool& coords_retrieved) const;

template void ArraySchema::get_next_tile_coords<int>(
    const int* domain, int* tile_coords) const;
template void ArraySchema::get_next_tile_coords<int64_t>(
    const int64_t* domain, int64_t* tile_coords) const;
template void ArraySchema::get_next_tile_coords<float>(
    const float* domain, float* tile_coords) const;
template void ArraySchema::get_next_tile_coords<double>(
    const double* domain, double* tile_coords) const;
template void ArraySchema::get_next_tile_coords<int8_t>(
    const int8_t* domain, int8_t* tile_coords) const;
template void ArraySchema::get_next_tile_coords<uint8_t>(
    const uint8_t* domain, uint8_t* tile_coords) const;
template void ArraySchema::get_next_tile_coords<int16_t>(
    const int16_t* domain, int16_t* tile_coords) const;
template void ArraySchema::get_next_tile_coords<uint16_t>(
    const uint16_t* domain, uint16_t* tile_coords) const;
template void ArraySchema::get_next_tile_coords<uint32_t>(
    const uint32_t* domain, uint32_t* tile_coords) const;
template void ArraySchema::get_next_tile_coords<uint64_t>(
    const uint64_t* domain, uint64_t* tile_coords) const;

template void ArraySchema::get_previous_cell_coords<int>(
    const int* domain, int* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<int64_t>(
    const int64_t* domain, int64_t* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<float>(
    const float* domain, float* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<double>(
    const double* domain, double* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<int8_t>(
    const int8_t* domain, int8_t* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<uint8_t>(
    const uint8_t* domain, uint8_t* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<int16_t>(
    const int16_t* domain, int16_t* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<uint16_t>(
    const uint16_t* domain, uint16_t* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<uint32_t>(
    const uint32_t* domain, uint32_t* cell_coords) const;
template void ArraySchema::get_previous_cell_coords<uint64_t>(
    const uint64_t* domain, uint64_t* cell_coords) const;

template void ArraySchema::get_subarray_tile_domain<int>(
    const int* subarray, int* tile_domain, int* subarray_tile_domain) const;
template void ArraySchema::get_subarray_tile_domain<int64_t>(
    const int64_t* subarray,
    int64_t* tile_domain,
    int64_t* subarray_tile_domain) const;
template void ArraySchema::get_subarray_tile_domain<int8_t>(
    const int8_t* subarray,
    int8_t* tile_domain,
    int8_t* subarray_tile_domain) const;
template void ArraySchema::get_subarray_tile_domain<uint8_t>(
    const uint8_t* subarray,
    uint8_t* tile_domain,
    uint8_t* subarray_tile_domain) const;
template void ArraySchema::get_subarray_tile_domain<int16_t>(
    const int16_t* subarray,
    int16_t* tile_domain,
    int16_t* subarray_tile_domain) const;
template void ArraySchema::get_subarray_tile_domain<uint16_t>(
    const uint16_t* subarray,
    uint16_t* tile_domain,
    uint16_t* subarray_tile_domain) const;
template void ArraySchema::get_subarray_tile_domain<uint32_t>(
    const uint32_t* subarray,
    uint32_t* tile_domain,
    uint32_t* subarray_tile_domain) const;
template void ArraySchema::get_subarray_tile_domain<uint64_t>(
    const uint64_t* subarray,
    uint64_t* tile_domain,
    uint64_t* subarray_tile_domain) const;

template int64_t ArraySchema::get_tile_pos<int>(
    const int* domain, const int* tile_coords) const;
template int64_t ArraySchema::get_tile_pos<int64_t>(
    const int64_t* domain, const int64_t* tile_coords) const;
template int64_t ArraySchema::get_tile_pos<float>(
    const float* domain, const float* tile_coords) const;
template int64_t ArraySchema::get_tile_pos<double>(
    const double* domain, const double* tile_coords) const;
template int64_t ArraySchema::get_tile_pos<int8_t>(
    const int8_t* domain, const int8_t* tile_coords) const;
template int64_t ArraySchema::get_tile_pos<uint8_t>(
    const uint8_t* domain, const uint8_t* tile_coords) const;
template int64_t ArraySchema::get_tile_pos<int16_t>(
    const int16_t* domain, const int16_t* tile_coords) const;
template int64_t ArraySchema::get_tile_pos<uint16_t>(
    const uint16_t* domain, const uint16_t* tile_coords) const;
template int64_t ArraySchema::get_tile_pos<uint32_t>(
    const uint32_t* domain, const uint32_t* tile_coords) const;
template int64_t ArraySchema::get_tile_pos<uint64_t>(
    const uint64_t* domain, const uint64_t* tile_coords) const;

template void ArraySchema::get_tile_subarray<int>(
    const int* tile_coords, int* tile_subarray) const;
template void ArraySchema::get_tile_subarray<int64_t>(
    const int64_t* tile_coords, int64_t* tile_subarray) const;
template void ArraySchema::get_tile_subarray<int8_t>(
    const int8_t* tile_coords, int8_t* tile_subarray) const;
template void ArraySchema::get_tile_subarray<uint8_t>(
    const uint8_t* tile_coords, uint8_t* tile_subarray) const;
template void ArraySchema::get_tile_subarray<int16_t>(
    const int16_t* tile_coords, int16_t* tile_subarray) const;
template void ArraySchema::get_tile_subarray<uint16_t>(
    const uint16_t* tile_coords, uint16_t* tile_subarray) const;
template void ArraySchema::get_tile_subarray<uint32_t>(
    const uint32_t* tile_coords, uint32_t* tile_subarray) const;
template void ArraySchema::get_tile_subarray<uint64_t>(
    const uint64_t* tile_coords, uint64_t* tile_subarray) const;

template bool ArraySchema::is_contained_in_tile_slab_col<int>(
    const int* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<int64_t>(
    const int64_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<float>(
    const float* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<double>(
    const double* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<int8_t>(
    const int8_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<uint8_t>(
    const uint8_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<int16_t>(
    const int16_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<uint16_t>(
    const uint16_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<uint32_t>(
    const uint32_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_col<uint64_t>(
    const uint64_t* range) const;

template bool ArraySchema::is_contained_in_tile_slab_row<int>(
    const int* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<int64_t>(
    const int64_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<float>(
    const float* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<double>(
    const double* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<int8_t>(
    const int8_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<uint8_t>(
    const uint8_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<int16_t>(
    const int16_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<uint16_t>(
    const uint16_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<uint32_t>(
    const uint32_t* range) const;
template bool ArraySchema::is_contained_in_tile_slab_row<uint64_t>(
    const uint64_t* range) const;

template int ArraySchema::subarray_overlap<int>(
    const int* subarray_a, const int* subarray_b, int* overlap_subarray) const;
template int ArraySchema::subarray_overlap<int64_t>(
    const int64_t* subarray_a,
    const int64_t* subarray_b,
    int64_t* overlap_subarray) const;
template int ArraySchema::subarray_overlap<float>(
    const float* subarray_a,
    const float* subarray_b,
    float* overlap_subarray) const;
template int ArraySchema::subarray_overlap<double>(
    const double* subarray_a,
    const double* subarray_b,
    double* overlap_subarray) const;
template int ArraySchema::subarray_overlap<int8_t>(
    const int8_t* subarray_a,
    const int8_t* subarray_b,
    int8_t* overlap_subarray) const;
template int ArraySchema::subarray_overlap<uint8_t>(
    const uint8_t* subarray_a,
    const uint8_t* subarray_b,
    uint8_t* overlap_subarray) const;
template int ArraySchema::subarray_overlap<int16_t>(
    const int16_t* subarray_a,
    const int16_t* subarray_b,
    int16_t* overlap_subarray) const;
template int ArraySchema::subarray_overlap<uint16_t>(
    const uint16_t* subarray_a,
    const uint16_t* subarray_b,
    uint16_t* overlap_subarray) const;
template int ArraySchema::subarray_overlap<uint32_t>(
    const uint32_t* subarray_a,
    const uint32_t* subarray_b,
    uint32_t* overlap_subarray) const;
template int ArraySchema::subarray_overlap<uint64_t>(
    const uint64_t* subarray_a,
    const uint64_t* subarray_b,
    uint64_t* overlap_subarray) const;

template int ArraySchema::tile_cell_order_cmp<int>(
    const int* coords_a, const int* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<int64_t>(
    const int64_t* coords_a, const int64_t* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<float>(
    const float* coords_a, const float* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<double>(
    const double* coords_a, const double* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<int8_t>(
    const int8_t* coords_a, const int8_t* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<uint8_t>(
    const uint8_t* coords_a, const uint8_t* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<int16_t>(
    const int16_t* coords_a, const int16_t* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<uint16_t>(
    const uint16_t* coords_a, const uint16_t* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<uint32_t>(
    const uint32_t* coords_a, const uint32_t* coords_b) const;
template int ArraySchema::tile_cell_order_cmp<uint64_t>(
    const uint64_t* coords_a, const uint64_t* coords_b) const;

template int64_t ArraySchema::tile_id<int>(const int* cell_coords) const;
template int64_t ArraySchema::tile_id<int64_t>(
    const int64_t* cell_coords) const;
template int64_t ArraySchema::tile_id<float>(const float* cell_coords) const;
template int64_t ArraySchema::tile_id<double>(const double* cell_coords) const;
template int64_t ArraySchema::tile_id<int8_t>(const int8_t* cell_coords) const;
template int64_t ArraySchema::tile_id<uint8_t>(
    const uint8_t* cell_coords) const;
template int64_t ArraySchema::tile_id<int16_t>(
    const int16_t* cell_coords) const;
template int64_t ArraySchema::tile_id<uint16_t>(
    const uint16_t* cell_coords) const;
template int64_t ArraySchema::tile_id<uint32_t>(
    const uint32_t* cell_coords) const;
template int64_t ArraySchema::tile_id<uint64_t>(
    const uint64_t* cell_coords) const;

}  // namespace tiledb
