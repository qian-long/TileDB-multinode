#include <string>
#ifndef LOG_H
#define LOG_H

class Logger {
  public:

    // CONSTRUCTOR
    Logger(std::string logfile);

    // DESTRUCTOR
    ~Logger();


    // LOG METHODS
    void log(std::string message);

    // GETTERS
    std::string get_logfile();

    // HELPERS
    std::string current_timestring();

  private:
    std::string logfile_;


};

#endif
