#include <string>
#ifndef LOG_H
#define LOG_H

#define LOG_STRINGIFY(x) #x
#define LOG_TOSTRING(x) LOG_STRINGIFY(x)
#define LOG_INFO __FILE__ ":" LOG_TOSTRING(__LINE__)


class Logger {
  public:

    // CONSTRUCTOR
    Logger(std::string logfile);

    // DESTRUCTOR
    ~Logger();


    // LOG METHODS
    void log(std::string log_info, std::string message);

    // GETTERS
    std::string get_logfile();

    // HELPERS
    std::string current_timestring();

  private:
    std::string logfile_;


};

#endif
