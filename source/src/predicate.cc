#include "predicate.h"
#include "array_schema.h" // TODO fix this later by moving datatype out
#include <sstream>
#include <typeinfo>
#include <iostream>
#include "debug.h"
#include <cstring>

template<class T>
Predicate<T>::Predicate(int attr_index, Op op, T operand) {
  attr_index_ = attr_index;
  op_ = op;
  operand_ = operand;
};


template<class T>
Predicate<T>::~Predicate() {};

template<class T>
std::string Predicate<T>::serialize() {

  int length = sizeof(int) + sizeof(Op) + sizeof(T);
  char buffer[length];
  int pos = 0;

  // serialize attribute index
  memcpy(&buffer[0], &attr_index_, sizeof(int));
  pos += sizeof(int);

  // serialize operator
  memcpy(&buffer[pos], &op_, sizeof(Op));
  pos += sizeof(Op);

  // serialize operand
  memcpy(&buffer[pos], &operand_, sizeof(T));
  return std::string(buffer, length);
}

template<class T>
Predicate<T>* Predicate<T>::deserialize(const char* buffer, int length) {
  int pos = 0;

  int attr_index = (int) buffer[pos]; // attribute index
  pos += sizeof(int);

  Op op = static_cast<Op>(buffer[pos]); // operator
  pos += sizeof(Op);

  T operand;
  // not sure why type casting doesn't work
  // ex) T operand = (T) buffer[pos] or static_cast<T>(buffer[pos])
  memcpy(&operand, &buffer[pos], sizeof(T)); // operand

  return new Predicate<T>(attr_index, op, operand);
}

template<class T>
std::string Predicate<T>::to_string() {
  std::stringstream ss;
  ss << "Predicate{attr_index: " << attr_index_ << ", op: ";
    
  switch(op_) {
    case LT:
      ss << "LT";
      break;
    case LE:
      ss << "LE";
      break;
    case EQ:
      ss << "EQ";
      break;
    case GE:
      ss << "GE";
      break;
    case GT:
      ss << "GT";
      break;
    case NE:
      ss << "NE";
      break;
    default:
      ss << "INVALID OP";
      break;
  }

  ss << ", operand: " << operand_ << "}\n";

  return ss.str();
}

template class Predicate<int>;
template class Predicate<float>;
template class Predicate<double>;
// TODO why does this complain?
//template class Predicate<int64_t>;

