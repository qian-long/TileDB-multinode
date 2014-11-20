#include "loader.h"
#include "query_processor.h"
#include <iostream>

// Simply initializes schemas A, B, R_A, R_B.
// A and R_A are arrays with regular tiles, 
// whereas B and R_B are arrays with irregular tiles
void get_array_schemas(ArraySchema*& array_schema_A,
                       ArraySchema*& array_schema_B,
                       ArraySchema*& array_schema_R_A,
                       ArraySchema*& array_schema_R_B) {
  // Attribute names
  std::vector<std::string> attribute_names;
  attribute_names.push_back("attr1");
  attribute_names.push_back("attr2");

  // Attribute types
  std::vector<ArraySchema::DataType> attribute_types;
  attribute_types.push_back(ArraySchema::INT);
  attribute_types.push_back(ArraySchema::INT);

  // Dimension domains
  std::vector<std::pair<double, double> > dim_domains;
  dim_domains.push_back(std::pair<double, double>(0,999));
  dim_domains.push_back(std::pair<double, double>(0,999));

  // Dimension names
  std::vector<std::string> dim_names;
  dim_names.push_back("dim1");
  dim_names.push_back("dim2");

  // Dimension type
  ArraySchema::DataType dim_type = ArraySchema::INT;

  // Tile extents for the regular array
  std::vector<double> tile_extents;
  tile_extents.push_back(10);
  tile_extents.push_back(10);
 
  // Create array schemas  
  array_schema_A = new ArraySchema("A", attribute_names, attribute_types,
                                   dim_domains, dim_names, dim_type,
                                   tile_extents);
  array_schema_B = new ArraySchema("B", attribute_names, attribute_types,
                                   dim_domains, dim_names, dim_type);
  array_schema_R_A = new ArraySchema("R_A", attribute_names, attribute_types,
                                     dim_domains, dim_names, dim_type,
                                     tile_extents);
  array_schema_R_B = new ArraySchema("R_B", attribute_names, attribute_types,
                                     dim_domains, dim_names, dim_type);
}

void get_range(std::vector<double>& range) {
  range.push_back(9);
  range.push_back(11);
  range.push_back(10);
  range.push_back(13);
}

// Properly comment out the commands that you want to be executed.
int main() {
  // Initialize some example array schemas
  ArraySchema *array_schema_A, *array_schema_B;
  ArraySchema *array_schema_R_A, *array_schema_R_B;
  get_array_schemas(array_schema_A, array_schema_B, 
                    array_schema_R_A, array_schema_R_B);

  try {
    // Create a storage manager. The input is a path that MUST exist. 
    StorageManager storage_manager("~/projects/TileDB-multinode/Data");

    // Create a loader. The input is a path that MUST exist, and a 
    // storage manager. 
    Loader loader("~/projects/TileDB-multinode/Data", storage_manager);

    // Create a query processor. The input is a storage manager. 
    QueryProcessor query_processor(storage_manager);

    // Load array from a CSV file
    // Make sure the CSV files in the path exist.
    loader.load("~/projects/TileDB-multinode/Data/test.csv",
                 *array_schema_A, Loader::HILBERT);

    loader.load("~/projects/TileDB-multinode/Data/test.csv",
                 *array_schema_B, Loader::COLUMN_MAJOR);




    // Testing predicate
    int attr_index = 0;
    Op op = LT;
    int operand = 8;
    Predicate<int> int_pred(attr_index, op, operand);
    bool result = query_processor.evaluate_predicate<int>(7, int_pred);
    std::cout << "int pred result: " << result << "\n";

    Op fop = NE;
    float foperand = 6.0;
    Predicate<float> float_pred(attr_index, fop, foperand);
    bool fresult = query_processor.evaluate_predicate<float>(6.0, float_pred);
    std::cout << "float pred result: " << fresult << "\n";


    /*
    // Export an array to a CSV file
    query_processor.export_to_CSV(
        *array_schema_B, 
        "~/projects/TileDB-multinode/Data/B_exported.csv");
 
    // Process a subarray query
    std::vector<double> range;
    get_range(range);
    query_processor.subarray(*array_schema_A, range, 
                             array_schema_R_A->array_name());

    query_processor.export_to_CSV(*array_schema_R_A,
                                  "~/projects/TileDB-multinode/Data/R_A.csv");
    query_processor.subarray(*array_schema_B, range, 
                            array_schema_R_B->array_name());
    query_processor.export_to_CSV(*array_schema_R_B, 
                                  "~/projects/TileDB-multinode/Data/R_B.csv");
                                  */

    // Delete an array
    storage_manager.delete_array(array_schema_R_A->array_name());
    storage_manager.delete_array(array_schema_R_B->array_name());

    // Clean up
    delete array_schema_A;
    delete array_schema_B;
    delete array_schema_R_A;
    delete array_schema_R_B;

  // Catching exceptions 
  } catch(StorageManagerException& sme) {
    delete array_schema_A;
    delete array_schema_B;
    delete array_schema_R_A;
    delete array_schema_R_B;
    std::cout << sme.what() << "\n";
  } catch(LoaderException& le) {
    delete array_schema_A;
    delete array_schema_B;
    delete array_schema_R_A;
    delete array_schema_R_B;
    std::cout << le.what() << "\n";
  } catch(QueryProcessorException& qpe) {
    delete array_schema_A;
    delete array_schema_B;
    delete array_schema_R_A;
    delete array_schema_R_B;
    std::cout << qpe.what() << "\n";
  }

  return 0;
}
