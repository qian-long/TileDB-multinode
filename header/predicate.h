#ifndef PREDICATE_H
#define PREDICATE_H

// TODO figure out how to move inside Predicate class
enum Op {LT, LE, EQ, GE, GT, NE};

template<class T>
class Predicate {
  public:

    Predicate(int attr_index, Op op, T operand);
    ~Predicate();

    int attr_index;
    Op op;
    T operand;


};

#endif
