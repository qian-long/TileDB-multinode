#include <time.h>
#include <iostream>
#include <fstream>
#include "logger.h"
#include "debug.h"


Logger::Logger(std::string logfile) {
  logfile_ = logfile;
}


Logger::~Logger() {};

std::string Logger::get_logfile() {
  return logfile_;
}


void Logger::log(std::string log_info, std::string message) {
  std::string cur_time = current_timestring();
  std::ofstream myfile;
  myfile.open (logfile_, std::ios::out | std::ios::app);

  myfile << "[" << cur_time << "] " << log_info << ": " << message << "\n";
  myfile.close();  
  
  LOGGER_DEBUG_MSG(message, log_info);
}

std::string Logger::current_timestring() {
  time_t rawtime;
  struct tm * timeinfo;
  char buffer [100];

  time(&rawtime);
  timeinfo = localtime (&rawtime);

  int len = strftime(buffer, 100,"%c",timeinfo);

  std::string timestring = std::string(buffer, len);
  return timestring;
}
