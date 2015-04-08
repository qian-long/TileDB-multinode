#include <time.h>
#include <iostream>
#include <fstream>
#include <sys/time.h>
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
  myfile.open(logfile_.c_str(), std::ios::out | std::ios::app);

  myfile << "[" << cur_time << "] " << log_info << ": " << message << "\n";
  myfile.close();  
  
  LOGGER_DEBUG_MSG(message, log_info);
}

void Logger::log_start(std::string log_info, std::string message) {
  struct timeval tim;
  gettimeofday(&tim, NULL);
  start_time_ = tim.tv_sec+(tim.tv_usec/1000000.0);
  time_message_ = message;
  log(log_info, "LOG_START " + message);
}

void Logger::log_end(std::string log_info) {
  struct timeval tim;
  gettimeofday(&tim, NULL);
  double tend = tim.tv_sec+(tim.tv_usec/1000000.0);

  std::stringstream ss;
  ss << "LOG_END " << time_message_ << " DURATION: " << tend - start_time_ << " secs";
  log(log_info, ss.str());
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

