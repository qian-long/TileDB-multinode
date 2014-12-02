#include "loader.h"
#include "query_processor.h"
#include <iostream>

int main() {

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
  ArraySchema *array_schema_reg = new ArraySchema("test1_reg", attribute_names, attribute_types,
                                   dim_domains, dim_names, dim_type,
                                   tile_extents);
 
  ArraySchema *array_schema_irreg = new ArraySchema("test1_irreg", attribute_names, attribute_types,
                                   dim_domains, dim_names, dim_type);
 
  ArraySchema *array_schema_filter = new ArraySchema("test1_filter", attribute_names, attribute_types, dim_domains, dim_names, dim_type);
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
                 *array_schema_irreg, Loader::ROW_MAJOR);




    // Testing predicate
    int attr_index = 1;
    Op op = GT;
    int operand = 8;
    Predicate<int> pred_lt_4(attr_index, op, operand);


    // Process a filter query
    query_processor.filter_irregular<int>(*array_schema_irreg, pred_lt_4, array_schema_filter->array_name());
    query_processor.export_to_CSV(*array_schema_filter, "~/projects/TileDB-multinode/Data/output/filter_irreg_test.csv");


    // testing serializing predicate
    std::string serial = pred_lt_4.serialize();
    std::cout << "serial: " << serial << "\n";

    Predicate<int> *pred = Predicate<int>::deserialize(serial.c_str(), serial.size());
    std::cout << "pred.attr_index: " << pred->attr_index << "\n";
    std::cout << "pred.op: " << pred->op << "\n";
    std::cout << "pred.operand: " << pred->operand << "\n";

  // Catching exceptions 
  } catch(StorageManagerException& sme) {

    delete array_schema_irreg;
    delete array_schema_reg;

    std::cout << "StorageManagerException:\n";
    std::cout << sme.what() << "\n";
  } catch(LoaderException& le) {

    delete array_schema_irreg;
    delete array_schema_reg;
 
    std::cout << "LoaderException:\n";
    std::cout << le.what() << "\n";
  } catch(QueryProcessorException& qpe) {

    delete array_schema_irreg;
    delete array_schema_reg;
 
    std::cout << "QueryProcessorException:\n";
    std::cout << qpe.what() << "\n";
  }


}
