#include "predicate.h"
#include "array_schema.h" // TODO fix this later by moving datatype out
#include <sstream>
#include <typeinfo>
#include <iostream>

template<class T>
Predicate<T>::Predicate(int attr_index, Op op, T operand) {
  this->attr_index = attr_index;
  this->op = op;
  this->operand = operand;
};


template<class T>
Predicate<T>::~Predicate() {};

template<class T>
std::string Predicate<T>::serialize() {

  std::stringstream ss;

  // serialize attribute index
  ss.write((char *) &this->attr_index, sizeof(int));

  // serialize operator
  ss.write((char *) &this->op, sizeof(Op));

  // serialize operand type
  
  // serialize operand
  ss.write((char *) &this->operand, sizeof(T));

  return ss.str();
}

template<class T>
Predicate<T>* Predicate<T>::deserialize(const char* buffer, int length) {
  std::stringstream ss;
  int pos = 0;

  int attr_index = (int) buffer[pos]; // attribute index
  pos += sizeof(int);

  Op op = static_cast<Op>(buffer[pos]); // operator
  pos += sizeof(Op);

  T operand = (T) buffer[pos];
  return new Predicate<T>(attr_index, op, operand);
}

template<class T>
std::string Predicate<T>::to_string() {
  std::stringstream ss;
  ss << "Predicate{attr_index: " << attr_index << ", op: " << op << ", operand: " << operand << "}\n";
  return ss.str();
}

template class Predicate<int>;
template class Predicate<float>;
template class Predicate<double>;
// TODO why does this complain?
//template class Predicate<int64_t>;

