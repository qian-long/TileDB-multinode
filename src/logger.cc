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
  tstart_wall_ = get_wall_time();
  tstart_cpu_ = get_cpu_time();
  time_message_ = message;
  log(log_info, "LOG_START " + message);
}

void Logger::log_end(std::string log_info) {
  struct timeval tim;
  gettimeofday(&tim, NULL);
  double tend_wall = get_wall_time();
  double tend_cpu = get_cpu_time();

  std::stringstream ss;
  ss << "LOG_END {" << time_message_ << "} WALL TIME: {" << tend_wall - tstart_wall_ << "} secs " << "CPU TIME: {" << tend_cpu - tstart_cpu_ << "} secs ";
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

double Logger::get_wall_time() {
  struct timeval time;
  if (gettimeofday(&time,NULL)) {
    //Handle error
    return 0;
  }
  return (double)time.tv_sec + (double)time.tv_usec * .000001;
}

double Logger::get_cpu_time() {
  return (double)clock() / CLOCKS_PER_SEC;
}

