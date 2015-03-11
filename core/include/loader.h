/**
 * @file   loader.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file defines class Loader. It also defines LoaderException, which is 
 * thrown by Loader.
 */

#ifndef LOADER_H
#define LOADER_H

#include "array_schema.h"
#include "csv_file.h"
#include "storage_manager.h"

/** Special value indicating an invalid tile id. */
#define LD_INVALID_TILE_ID std::numeric_limits<uint64_t>::max()

/**
 * The Loader is the module that creates the array layout from raw data 
 * presented in a CSV format. It can load data into arrays with both 
 * regular and irregular tiles, supporting various cell and tile orders.
 */
class Loader {
 public:
  // CONSTRUCTORS AND DESTRUCTORS
  /** 
   * Simple constructor. The workspace is where the loader will create its
   * data. The storage manager is the module the loader interefaces with.
   */
  Loader(const std::string& workspace, StorageManager& storage_manager);
  /** Empty destructor. */
  ~Loader() {}

  // LOADING FUNCTIONS
  /** Creates a fragment from a CSV file. */
  void load(const std::string& filename, 
            const std::string& array_name, 
            const std::string& fragment_name) const;

  /**  Sorts the csv file depending on the type of tiles and order. */
  void sort_csv_file(const std::string& to_be_sorted_filename,
                     const std::string& sorted_filename,
                     const ArraySchema& array_schema) const;

  /**
   * Injects tile/cell ids to the CSV file prior to loading (applies only to
   * regular tiles with any order, and irregular tiles with Hilbert order). 
   */
  void inject_ids_to_csv_file(const std::string& filename, 
                              const std::string& injected_filename,
                              const ArraySchema& array_schema) const;

  std::string workspace() { return workspace_; }

  /*
  void make_tiles_irregular(const std::string& filename,
                            const StorageManager::ArrayDescriptor* ad,
                            const ArraySchema& array_schema) const;
  void make_tiles_regular(const std::string& filename,
                          const StorageManager::ArrayDescriptor* ad,
                          const ArraySchema& array_schema) const;
                          */

  /** Creates the (irregular) tiles and sends them to the storage manager. */
  void make_tiles_irregular(const std::string& filename,
                            const StorageManager::FragmentDescriptor* fd) const;
  /** Creates the (regular) tiles and sends them to the storage manager. */
  void make_tiles_regular(const std::string& filename,
                          const StorageManager::FragmentDescriptor* fd) const;
 
 private:
  // PRIVATE ATTRIBUTES
  /** The StorageManager object the loader interfaces with. */
  StorageManager& storage_manager_;
  /** A folder in the disk where the loader creates all its data. */
  std::string workspace_;
 
  // PRIVATE METHODS
  /** 
   * Treats the CSV line as a logical cell encompassing coordinates and
   * attribute values, and appends the coordinates to a coordinate tile
   * and the attribute values to the respective attribute tiles. 
   */
  void append_cell(const ArraySchema& array_schema, 
                    CSVLine* csv_line, Tile** tiles) const;
  /** Checks upon invoking the load command. */
  bool check_on_load(const std::string& filename) const;
  /** Creates the workspace folder. */
  void create_workspace() const;

 /** 
   * Creates an array of new tile pointers with the input tile id, 
   * and based on the input array schema. 
   */
  void new_tiles(const ArraySchema& array_schema, 
                 uint64_t tile_id, Tile** tiles) const;
  /** Returns true if the input path is an existing directory. */
  bool path_exists(const std::string& path) const;
  /** Simply sets the workspace. */
  void set_workspace(const std::string& path);
  /** Sends the tiles to the storage manager. */
  void store_tiles(const StorageManager::FragmentDescriptor* fd,
                   Tile** tiles) const;
};

/** This exception is thrown by Loader. */
class LoaderException {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Takes as input the exception message. */
  LoaderException(const std::string& msg) 
      : msg_(msg) {}
  /** Empty destructor. */
  ~LoaderException() {}

  // ACCESSORS
  /** Returns the exception message. */
  const std::string& what() const { return msg_; }

 private:
  /** The exception message. */
  std::string msg_;
};

#endif
