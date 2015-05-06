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
    // sets start_time to current timte
    void log_start(std::string log_info, std::string message);
    // takes difference of current time and start time
    void log_end(std::string log_info);

    // GETTERS
    std::string get_logfile();

    // HELPERS
    std::string current_timestring();

  private:
    std::string logfile_;

    // current timing message
    std::string time_message_;

    // current timing start (in seconds)
    double tstart_wall_;
    double tstart_cpu_;

    // helper methods
    double get_wall_time();
    double get_cpu_time();

};

#endif
