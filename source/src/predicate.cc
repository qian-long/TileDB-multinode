#include "predicate.h"

template<class T>
Predicate<T>::Predicate(int attr_index, Op op, T operand) {
  this->attr_index = attr_index;
  this->op = op;
  this->operand = operand;
};


template<class T>
Predicate<T>::~Predicate() {};


template class Predicate<int>;
template class Predicate<float>;
template class Predicate<double>;
// TODO why does this complain?
//template class Predicate<int64_t>;

