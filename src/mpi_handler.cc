#include <fstream>
#include "mpi.h"
#include "mpi_handler.h"
#include "csv_file.h"
#include "constants.h"
#include "messages.h"

MPIHandler::MPIHandler() {

}

MPIHandler::~MPIHandler() {

}

void MPIHandler::send_file(std::string filepath, int receiver, int tag) {
  CSVFile file(filepath, CSVFile::READ, MAX_DATA); 
  CSVLine line;
  std::stringstream content;
  bool keep_receiving = true;

  // TODO throw exception?
  while(file >> line) {
    // encode "there is more coming" in the last byte
    if (content.str().length() + line.str().length() + 1 >= MAX_DATA) {
      content.write((char *) &keep_receiving, sizeof(bool));
      MPI_Send(content.str().c_str(), content.str().length(), 
          MPI_CHAR, receiver, tag, MPI_COMM_WORLD);
      content.str(std::string()); // clear buffer
    }
    content << line.str() << "\n";
  }

  // final send, tell coordinator to stop receiving
  keep_receiving = false;
  content.write((char *) &keep_receiving, sizeof(bool));
  MPI_Send(content.str().c_str(), content.str().length(), 
      MPI_CHAR, receiver, tag, MPI_COMM_WORLD);

}

void MPIHandler::receive_file(std::ofstream& file, int sender, int tag) {
  bool keep_receiving = true;
  char *buf = new char[MAX_DATA];
  MPI_Status status;
  int length;
  int content_length;

  do {
    MPI_Recv(buf, MAX_DATA, MPI_CHAR, sender, tag, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_CHAR, &length);

    content_length = length - 1;

    // check last byte to see if keep receiving
    keep_receiving = (bool) buf[content_length];

    // write to file
    file << std::string(buf, content_length);

  } while (keep_receiving);


  // TODO add flushing?
  // Cleanup
  delete [] buf;
}
