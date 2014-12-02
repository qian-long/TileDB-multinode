#ifndef PREDICATE_H
#define PREDICATE_H
#include <string>

// TODO figure out how to move inside Predicate class
enum Op {LT, LE, EQ, GE, GT, NE};

template<class T>
class Predicate {
  public:
    // MEMBERS
    int attr_index;
    Op op;
    T operand;

    // CONSTRUCTOR
    Predicate(int attr_index, Op op, T operand);

    // DESTRUCTOR
    ~Predicate();

    // METHODS
    std::string serialize();
    static Predicate<T>* deserialize(const char* buffer, int length);
};

#endif
