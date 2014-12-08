#include "loader.h"
#include "query_processor.h"
#include <iostream>
#include "messages.h"

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
  dim_domains.push_back(std::pair<double, double>(0,1000));
  dim_domains.push_back(std::pair<double, double>(0,1000));

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
  ArraySchema *array_schema_irreg = new ArraySchema("irreg", attribute_names, attribute_types,
                                   dim_domains, dim_names, dim_type);
 
  ArraySchema *array_schema_filter = new ArraySchema("filter", attribute_names, attribute_types, dim_domains, dim_names, dim_type);
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
    loader.load("~/projects/TileDB-multinode/Data/smallish.csv",
                 *array_schema_irreg, Loader::ROW_MAJOR);


    // Testing predicate
    int attr_index = 1;
    Op op = EQ;
    int operand = 500;
    Predicate<int> pred(attr_index, op, operand);


    // Process a filter query
    /*
    query_processor.filter_irregular<int>(*array_schema_irreg, pred, array_schema_filter->array_name());
    query_processor.export_to_CSV(*array_schema_filter, "~/projects/TileDB-multinode/Data/output/output.csv");
    */


    // testing serializing predicate
    /*
    std::string serial = pred.serialize();
    std::cout << "serial: " << serial << "\n";

    Predicate<int> *pred1 = Predicate<int>::deserialize(serial.c_str(), serial.size());
    std::cout << "pred.attr_index: " << pred1->attr_index_ << "\n";
    std::cout << "pred.op: " << pred1->op_ << "\n";
    std::cout << "pred.operand: " << pred1->operand_ << "\n";
    std::cout << "pred->to_string(): " << pred1->to_string() << "\n";
    */

    // testing serializing FilterMsg
    /*
    FilterMsg<int> fmsg = FilterMsg<int>(array_schema_irreg->attribute_type(attr_index), *array_schema_irreg, pred_lt_4, array_schema_filter->array_name()); 
    std::string blah = fmsg.serialize();

    FilterMsg<int> fmsg1 = FilterMsg<int>();
    FilterMsg<int>::deserialize(&fmsg1, blah.c_str(), blah.size());
    std::cout << "fmsg1.predicate_: " << fmsg1.predicate_.to_string() << "\n"; 
    std::cout << "fmsg1.array_schema_: " << fmsg1.array_schema_.to_string() << "\n"; 
    */

    double max = query_processor.aggregate(*array_schema_irreg, 1);
    std::cout << "max of attribute 1: " << max << "\n";
  // Catching exceptions 
  } catch(StorageManagerException& sme) {

    delete array_schema_irreg;

    std::cout << "StorageManagerException:\n";
    std::cout << sme.what() << "\n";
  } catch(LoaderException& le) {

    delete array_schema_irreg;
 
    std::cout << "LoaderException:\n";
    std::cout << le.what() << "\n";
  } catch(QueryProcessorException& qpe) {

    delete array_schema_irreg;
 
    std::cout << "QueryProcessorException:\n";
    std::cout << qpe.what() << "\n";
  }


}
