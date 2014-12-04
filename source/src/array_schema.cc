/**
 * @file   array_schema.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
#include "hilbert_curve.h"
#include "assert.h"
#include <math.h>
#include <algorithm>
#include <sstream>
#include <cstring>

/******************************************************
************ CONSTRUCTORS & DESTRUCTORS ***************
******************************************************/

ArraySchema::ArraySchema(
    const std::string& array_name,
    const std::vector<std::string>& attribute_names,
    const std::vector<DataType>& attribute_types,
    const std::vector<std::pair<double, double> >& dim_domains,
    const std::vector<std::string>& dim_names,
    DataType dim_type) {
  array_name_ = array_name;
  attribute_names_ = attribute_names;
  attribute_types_ = attribute_types;
  dim_domains_ = dim_domains;
  dim_names_ = dim_names;
  dim_type_ = dim_type;
 
  if(attribute_names_.size() != attribute_types_.size())
    throw ArraySchemaException("Cannot create ArraySchema object: "
                               "the number of attribute names is "
                               "different from the number of attribute "
                               "types.", array_name_);
 
  if(dim_names_.size() != dim_domains_.size())
    throw ArraySchemaException("Cannot create ArraySchema object: "
                               "the number of dimension names is "
                               "different from the number of dimension "
                               "domains.", array_name_);

  dim_num_ = dim_names_.size();
  attribute_num_ = attribute_names_.size();
  compute_hilbert_cell_bits();
}

ArraySchema::ArraySchema(
    const std::string& array_name,
    const std::vector<std::string>& attribute_names,
    const std::vector<DataType>& attribute_types,
    const std::vector<std::pair<double, double> >& dim_domains,
    const std::vector<std::string>& dim_names,
    DataType dim_type,
    const std::vector<double>& tile_extents) {
  array_name_ = array_name;
  attribute_names_ = attribute_names;
  attribute_types_ = attribute_types;
  dim_domains_ = dim_domains;
  dim_names_ = dim_names;
  dim_type_ = dim_type;
  tile_extents_ = tile_extents; 
 
  if(attribute_names_.size() != attribute_types_.size())
    throw ArraySchemaException("Cannot create ArraySchema object: "
                               "the number of attribute names is "
                               "different from the number of attribute "
                               "types.", array_name_);
  
  if(dim_names_.size() != dim_domains_.size())
    throw ArraySchemaException("Cannot create ArraySchema object: "
                               "the number of dimension names is "
                               "different from the number of dimension "
                               "domains.", array_name);
 
  if(dim_names_.size() != tile_extents_.size())
    throw ArraySchemaException("Cannot create ArraySchema object: "
                               "the number of dimension names is "
                               "different from the number of the tile "
                               "extents.", array_name_);

  for(unsigned int i=0; i<tile_extents_.size(); i++) { 
    if(tile_extents_[i] == 0)
      throw ArraySchemaException("Cannot create ArraySchema object: "
                                 "no tile extent can be zero.",
                                 array_name_);
    if(tile_extents_[i] > (dim_domains_[i].second - dim_domains_[i].first + 1))
      throw ArraySchemaException("Cannot create ArraySchema object: "
                                 "tile extent exceeds domain range.",
                                 array_name_);
  }

  dim_num_ = dim_names_.size();
  attribute_num_ = attribute_names_.size();
  compute_hilbert_cell_bits();
  compute_hilbert_tile_bits();
  compute_tile_id_offsets();
}

/******************************************************
********************** ACCESSORS **********************
******************************************************/

uint64_t ArraySchema::attribute_cell_size(unsigned int i) const {
  if(i < 0 || i >= attribute_num_)
    throw ArraySchemaException("Cannot get attribute cell size: index out "
                               "of bounds.", array_name_);

  if(attribute_type(i) == INT)
    return sizeof(int);
  if(attribute_type(i) == INT64_T)
    return sizeof(int64_t);
  if(attribute_type(i) == FLOAT)
    return sizeof(float);
  if(attribute_type(i) == DOUBLE)
    return sizeof(double); 
}

const std::string& ArraySchema::attribute_name(unsigned int i) const {
  if(i < 0 || i >= attribute_num_)
    throw ArraySchemaException("Cannot get attribute name: index out "
                               "of bounds.", array_name_);

  return attribute_names_[i];
}

const ArraySchema::DataType& ArraySchema::attribute_type(unsigned int i) const {
  if(i < 0 || i >= attribute_num_)
    throw ArraySchemaException("Cannot get attribute type: index out "
                               "of bounds.", array_name_);

  return attribute_types_[i];
}

uint64_t ArraySchema::coordinates_cell_size() const {
  if(dim_type() == INT)
    return dim_num_ * sizeof(int);
  if(dim_type() == INT64_T)
    return dim_num_ * sizeof(int64_t);
  if(dim_type() == FLOAT)
    return dim_num_ * sizeof(float);
  if(dim_type() == DOUBLE)
    return dim_num_ * sizeof(double); 
}

uint64_t ArraySchema::max_cell_size() const {
  uint64_t max_size = coordinates_cell_size();
  for(unsigned int i=0; i<attribute_num_; i++) {
    uint64_t size = attribute_cell_size(i);
    if(max_size < size)
      max_size = size;
  }

  return max_size;
}

const std::vector<double>& ArraySchema::tile_extents() const {
  if(has_irregular_tiles())
    throw ArraySchemaException("Cannot get tile extents: array has "
                               "irregular tiles.", array_name_);
  
  return tile_extents_;
}

/******************************************************
************************ MISC *************************
******************************************************/

template<typename T>
uint64_t ArraySchema::cell_id_hilbert(
    const std::vector<T>& coordinates) const {
  if(dim_num_ == 0)
    throw ArraySchemaException("Cannot get cell id: "
                               "the number of dimensions should be non-zero.",
                               array_name_);

  if(coordinates.size() != dim_num_) 
    throw ArraySchemaException("Cannot get cell id: the number of coordinates "
                               "does not match the number of array dimensions.",
                               array_name_); 

  for(unsigned int i=0; i<dim_num_; i++) 
    if(coordinates[i] < dim_domains_[i].first ||
       coordinates[i] > dim_domains_[i].second)
      throw ArraySchemaException("Cannot get cell id: coordinates out of "
                                 "domain.", array_name_); 
 
  HilbertCurve *hc = new HilbertCurve();
  int *coord = new int[dim_num_];
  
  for(unsigned int i = 0; i < dim_num_; i++) 
    coord[i] = static_cast<int>(coordinates[i]);

  uint64_t cell_ID = hc->AxestoLine(coord, hilbert_cell_bits_, dim_num_);	

  delete hc;
  delete [] coord;
	
  return cell_ID;
}

bool ArraySchema::has_irregular_tiles() const {
  return (tile_extents_.size() == 0);
}

bool ArraySchema::has_regular_tiles() const {
  return (tile_extents_.size() != 0);
}

bool ArraySchema::is_aligned_with(const ArraySchema& array_schema) const {
  if(has_irregular_tiles())
    throw ArraySchemaException("Alignment does not apply to arrays with "
                               "irregular tiles.", array_name_); 

  if(array_schema.has_irregular_tiles())
    throw ArraySchemaException("Alignment does not apply to arrays with "
                               "irregular tiles.", array_schema.array_name()); 

  // Check array domains
  const std::vector<std::pair<double, double> >& dim_domains = 
      array_schema.dim_domains();
  if(dim_domains_.size() != dim_domains.size())
    return false;
  for(unsigned int i=0; i<dim_domains_.size(); i++)
    if(dim_domains_[i].first != dim_domains[i].first || 
       dim_domains_[i].second != dim_domains[i].second)
      return false;

  // Check tile extents
  const std::vector<double>& tile_extents = array_schema.tile_extents();
  if(tile_extents_.size() != tile_extents.size())
    return false;
  for(unsigned int i=0; i<tile_extents_.size(); i++)
    if(tile_extents_[i] != tile_extents[i])
      return false;

  return true;
}

template<typename T>
uint64_t ArraySchema::tile_id_column_major(
    const std::vector<T>& coordinates) const {
  check_on_tile_id_request(coordinates);
 
  uint64_t tile_ID = 0;
  uint64_t partition_id;
  for(unsigned int i = 0; i < dim_num_; i++) {
    partition_id = floor(coordinates[i] / tile_extents_[i]);
    tile_ID += partition_id * tile_id_offsets_column_major_[i];
  }	

  return tile_ID;
}

template<typename T>
uint64_t ArraySchema::tile_id_hilbert(const std::vector<T>& coordinates) const {
  check_on_tile_id_request(coordinates);
  	
  HilbertCurve *hc = new HilbertCurve();
  int *coord = new int[dim_num_];

  for(unsigned int i = 0; i < dim_num_; i++) 
    coord[i] = static_cast<int>(coordinates[i]/tile_extents_[i]);

  uint64_t tile_ID = hc->AxestoLine(coord, hilbert_tile_bits_, dim_num_);	

  delete hc;
  delete [] coord;

  return tile_ID;
}

template<typename T>
uint64_t ArraySchema::tile_id_row_major(
    const std::vector<T>& coordinates) const {
  check_on_tile_id_request(coordinates);
 
  uint64_t tile_ID = 0;
  uint64_t partition_id;
  for(unsigned int i = 0; i < dim_num_; i++) {
    partition_id = floor(coordinates[i] / tile_extents_[i]);
    tile_ID += partition_id * tile_id_offsets_row_major_[i];
  }	

  return tile_ID;
}

std::string datatype_to_string(ArraySchema::DataType type) {
  switch(type) {
    case ArraySchema::INT:
      return "INT";
    case ArraySchema::INT64_T:
      return "INT64_T";
    case ArraySchema::FLOAT:
      return "FLOAT";
    case ArraySchema::DOUBLE:
      return "DOUBLE";
    default:
      break;
  }
  return "";

}
std::string ArraySchema::to_string() {
  std::stringstream ss;
  ss << "ARRAY_SCHEMA" << "\n";
  ss << "array_name: " << array_name_ << "\n";
  ss << "num attributes: " << attribute_num_ << "\n";
  ss << "attribute names: ";
  std::vector<std::string>::iterator anames_iter = attribute_names_.begin();
  for (; anames_iter != attribute_names_.end(); anames_iter++) {
    ss << *anames_iter << ", ";
  }
  ss << "\n";

  ss << "attribute types: ";
  std::vector<DataType>::iterator at_iter = attribute_types_.begin();
  for (; at_iter != attribute_types_.end(); at_iter++) {
    ss << datatype_to_string(*at_iter) << ", ";
  }
  ss << "\n";

  ss << "num dimensions(coordinates): " << dim_num_ << "\n";
  ss << "dimension type: " << datatype_to_string(dim_type_) << "\n";

  std::vector<std::string>::iterator dnames_iter = dim_names_.begin();
  std::vector< std::pair< double, double > >::iterator iter = dim_domains_.begin();
  ss << "dimension domains: ";
  for (; dnames_iter != dim_names_.end(); dnames_iter++, iter++) {
    ss << *dnames_iter << ": (" << (*iter).first << ", " << (*iter).second << "), ";
  }
  ss << "\n";

  if (has_irregular_tiles()) {
    ss << "irregular tiling\n";
  } else {
    ss << "regular tiling extents: ";
    std::vector<double>::iterator te_iter = tile_extents_.begin();
    for (; te_iter != tile_extents_.end(); te_iter++) {
      ss << (*te_iter) << ", ";
    }
  }
  ss << "END ARRAY_SCHEMA\n";
  return ss.str();
}

void ArraySchema::print() {
  std::cout << this->to_string();
}

std::string ArraySchema::serialize() {
  std::stringstream ss;
  int array_name_size = array_name_.size();
  ss.write((char *) &array_name_size, sizeof(int));
  ss.write((char *) array_name_.c_str(), array_name_size);
  ss.write((char *) &attribute_num_, sizeof(int));
  std::vector<std::string>::iterator anames_iter = attribute_names_.begin();
  // attribute name length, attribute name
  for (; anames_iter != attribute_names_.end(); anames_iter++) {
    int attr_name_size = (*anames_iter).size();
    ss.write((char *) &attr_name_size, sizeof(int));
    ss.write((char *) (*anames_iter).c_str(), attr_name_size);
  }

  // attribute type, enum
  std::vector<DataType>::iterator at_iter = attribute_types_.begin();
  for (; at_iter != attribute_types_.end(); at_iter++) {
    DataType type = *at_iter;
    ss.write((char *) &type, sizeof(DataType));
  }

  // number of dimensions
  ss.write((char *) &dim_num_, sizeof(int));
  // dimension type
  ss.write((char *) &dim_type_, sizeof(DataType));
  bool irregular = false;
  if (has_irregular_tiles()) {
    irregular = true;
  }
  //ss << irregular;
  ss.write((char *) &irregular, sizeof(bool));
  std::vector<std::string>::iterator dnames_iter = dim_names_.begin();
  // dimension name
  for (; dnames_iter != dim_names_.end(); dnames_iter++) {
    int dim_name_length = (*dnames_iter).size();
    ss.write((char *) &dim_name_length, sizeof(int));
    ss.write((char *) (*dnames_iter).c_str(), dim_name_length);
  }

  // dimension endpoints
  std::vector< std::pair< double, double > >::iterator iter = dim_domains_.begin();
  for (; iter != dim_domains_.end(); iter++) {
    double first = (*iter).first;
    double second = (*iter).second;

    ss.write((char *) &first, sizeof(double));
    ss.write((char *) &second, sizeof(double));
  }

  // tile extents for regular tiles
  if (tile_extents_.size() != 0) {
    std::vector<double>::iterator te_iter = tile_extents_.begin();
    for (; te_iter != tile_extents_.end(); te_iter++) {
      double extent = *te_iter;
      ss.write((char *) &extent, sizeof(double));
    }
  }

  return ss.str();
}

// parse buffer and construct array schema object
ArraySchema* ArraySchema::deserialize(const char * buffer, int length) {
  int counter = 0;
  std::string array_name;
  std::vector<std::string> attribute_names;
  std::vector<DataType> attribute_types;
  std::vector<std::pair<double, double > > dim_domains;
  std::vector<std::string> dim_names;
  DataType dim_type;
  std::vector<double> tile_extents;

  int array_name_length;
  memcpy(&array_name_length, &buffer[counter], sizeof(int));
  counter += sizeof(int);
  std::stringstream ss;
  ss.write(&buffer[counter], array_name_length);
  counter += array_name_length;

  array_name = ss.str();
  int num_attributes = (int) buffer[counter];
  counter += sizeof(int);

  for (int i = 0; i < num_attributes; i++) {
    ss.str(std::string()); // clearing ss
    int attr_name_length = (int) buffer[counter];
    counter += sizeof(int);
    ss.write(&buffer[counter], attr_name_length);
    attribute_names.push_back(ss.str());
    counter += attr_name_length;
  }

  for (int i = 0; i < num_attributes; i++) {
    ss.str(std::string()); // clearing ss
    DataType type = static_cast<DataType>(buffer[counter]);
    attribute_types.push_back(type);
    counter += sizeof(DataType);
  }

  int num_dimensions = (int) buffer[counter];
  counter += sizeof(int);
  dim_type = static_cast<DataType>(buffer[counter]);
  counter += sizeof(DataType);
  bool irregular = (bool) buffer[counter++];

  // dim_names
  for (int i = 0; i < num_dimensions; i++) {
    ss.str(std::string()); // clearing ss
    int dim_name_length = (int) buffer[counter];
    counter += sizeof(int);
    ss.write(&buffer[counter], dim_name_length);
    dim_names.push_back(ss.str());
    counter += dim_name_length;
  }

  // dim domains
  for (int i = 0; i < num_dimensions; i++) {
    double first;
    memcpy(&first, &buffer[counter], sizeof(double));
    counter += sizeof(double);
    double second;
    memcpy(&second, &buffer[counter], sizeof(double));
    counter += sizeof(double);
    dim_domains.push_back(std::pair<double, double>(first, second));
  }

  if (irregular == 0) {
    for (int i = 0; i < num_dimensions; i++) {
      double extent;
      memcpy(&extent, &buffer[counter], sizeof(double));
      tile_extents.push_back(extent);
      counter += sizeof(double);
    }
    return new ArraySchema(array_name,
      attribute_names,
      attribute_types,
      dim_domains,
      dim_names,
      dim_type,
      tile_extents);

  } else {

    return new ArraySchema(array_name,
      attribute_names,
      attribute_types,
      dim_domains,
      dim_names,
      dim_type);
  }
}

ArraySchema * ArraySchema::deep_copy(std::string new_array_name) {
  std::vector<std::string> attribute_names;
  std::vector<DataType> attribute_types;
  std::vector<std::pair<double, double > > dim_domains;
  std::vector<std::string> dim_names;
  DataType dim_type;
  std::vector<double> tile_extents;


  std::vector<std::string>::iterator attr_names_it = attribute_names_.begin();
  for (; attr_names_it != attribute_names_.end(); ++attr_names_it) {
    attribute_names.push_back(*attr_names_it);
  }

  std::vector<DataType>::iterator attr_type_it = attribute_types_.begin();
  for (; attr_type_it != attribute_types_.end(); ++attr_type_it) {
    attribute_types.push_back(*attr_type_it);
  }


  // dim_names
  std::vector<std::string>::iterator dim_names_it = dim_names_.begin();
  for (; dim_names_it != dim_names_.end(); ++dim_names_it) {
    dim_names.push_back(*dim_names_it);
  }


  // dim domains
  std::vector<std::pair<double, double > >::iterator dim_domain_it = dim_domains_.begin();
  for (; dim_domain_it != dim_domains_.end(); ++dim_domain_it) {
    dim_domains.push_back(std::pair<double, double>(dim_domain_it->first, dim_domain_it->second));
  }

  if (has_regular_tiles()) {
    std::vector<double>::iterator tile_ex_it = tile_extents_.begin();
    for (; tile_ex_it != tile_extents_.end(); ++tile_ex_it) {
      tile_extents.push_back(*tile_ex_it);
    }

    return new ArraySchema(new_array_name,
      attribute_names,
      attribute_types,
      dim_domains,
      dim_names,
      dim_type,
      tile_extents);

  } else {

    return new ArraySchema(new_array_name,
      attribute_names,
      attribute_types,
      dim_domains,
      dim_names,
      dim_type);
  }

}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

template<typename T>
void ArraySchema::check_on_tile_id_request(
    const std::vector<T>& coordinates) const {
  if(has_irregular_tiles())
    throw ArraySchemaException("Cannot get tile id: the array has irregular "
                               "tiles.", array_name_); 

  if(dim_num_ == 0)
    throw ArraySchemaException("Cannot get tile id: "
                               "the number of dimensions should be non-zero.",
                               array_name_);

  if(coordinates.size() != dim_num_) 
    throw ArraySchemaException("Cannot get tile id: the number of coordinates "
                               "does not match the number of array dimensions.",
                               array_name_); 

  for(unsigned int i=0; i<dim_num_; i++) 
    if(coordinates[i] < dim_domains_[i].first ||
       coordinates[i] > dim_domains_[i].second)
      throw ArraySchemaException("Cannot get tile id: coordinates out of "
                                 "domain.", array_name_); 
}

void ArraySchema::compute_hilbert_cell_bits() {
  double max_domain_range = 0;
  double domain_range;
  for(unsigned int i = 0; i < dim_num_; i++) {       
    domain_range = dim_domains_[i].second - dim_domains_[i].first + 1;
    if(max_domain_range < domain_range)
      max_domain_range = domain_range;
  }

  hilbert_cell_bits_ = ceil(log2(static_cast<uint64_t>(max_domain_range+0.5)));
}

void ArraySchema::compute_hilbert_tile_bits() {
  assert(has_regular_tiles());

  double max_domain_range = 0;
  double domain_range;
  for(unsigned int i = 0; i < dim_num_; i++) {       
    domain_range = (dim_domains_[i].second - dim_domains_[i].first + 1) /
                    tile_extents_[i];  
    if(max_domain_range < domain_range)
      max_domain_range = domain_range;
  }

  hilbert_tile_bits_ = ceil(log2(static_cast<uint64_t>(max_domain_range+0.5)));
}

void ArraySchema::compute_tile_id_offsets() {
  assert(has_regular_tiles());
  
  double domain_range;
  uint64_t partition_num; // Number of partitions on some axis
  uint64_t offset_row = 1;
  uint64_t offset_column = 1;

  tile_id_offsets_row_major_.push_back(offset_row);	
  tile_id_offsets_column_major_.push_back(offset_column);	

  for(unsigned int i=0; i<dim_num_-1 ; i++) {
    domain_range = dim_domains_[i].second - dim_domains_[i].first + 1;
    partition_num = ceil(domain_range / tile_extents_[i]);
    offset_row *= partition_num;
    tile_id_offsets_row_major_.push_back(offset_row);
   
    domain_range = dim_domains_[dim_num_-1-i].second - 
                   dim_domains_[dim_num_-1-i].first + 1;
    partition_num = ceil(domain_range / tile_extents_[dim_num_-1-i]);
    offset_column *= partition_num;
    tile_id_offsets_column_major_.push_back(offset_column);
  }
  
  std::reverse(tile_id_offsets_column_major_.begin(), 
               tile_id_offsets_column_major_.end());
}

// Explicit template instantiations
template uint64_t ArraySchema::cell_id_hilbert<int>(
    const std::vector<int>& coordinates) const;
template uint64_t ArraySchema::cell_id_hilbert<int64_t>(
    const std::vector<int64_t>& coordinates) const;
template uint64_t ArraySchema::cell_id_hilbert<float>(
    const std::vector<float>& coordinates) const;
template uint64_t ArraySchema::cell_id_hilbert<double>(
    const std::vector<double>& coordinates) const;
template uint64_t ArraySchema::tile_id_row_major<int>(
    const std::vector<int>& coordinates) const;
template uint64_t ArraySchema::tile_id_row_major<int64_t>(
    const std::vector<int64_t>& coordinates) const;
template uint64_t ArraySchema::tile_id_row_major<float>(
    const std::vector<float>& coordinates) const;
template uint64_t ArraySchema::tile_id_row_major<double>(
    const std::vector<double>& coordinates) const;
template uint64_t ArraySchema::tile_id_column_major<int>(
    const std::vector<int>& coordinates) const;
template uint64_t ArraySchema::tile_id_column_major<int64_t>(
    const std::vector<int64_t>& coordinates) const;
template uint64_t ArraySchema::tile_id_column_major<float>(
    const std::vector<float>& coordinates) const;
template uint64_t ArraySchema::tile_id_column_major<double>(
    const std::vector<double>& coordinates) const;
template uint64_t ArraySchema::tile_id_hilbert<int>(
    const std::vector<int>& coordinates) const;
template uint64_t ArraySchema::tile_id_hilbert<int64_t>(
    const std::vector<int64_t>& coordinates) const;
template uint64_t ArraySchema::tile_id_hilbert<float>(
    const std::vector<float>& coordinates) const;
template uint64_t ArraySchema::tile_id_hilbert<double>(
    const std::vector<double>& coordinates) const;
