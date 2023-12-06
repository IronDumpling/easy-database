// -*- c++ -*-
#ifndef RPCXX_H
#define RPCXX_H

#include <cstdlib>
#include "rpc.h"

namespace rpc {

// Protocol is used for encode and decode a type to/from the network.
//
// You may use network byte order, but it's optional. We won't test your code
// on two different architectures.

// TASK1: add more specializations to Protocol template class to support more
// types.
template <typename T> struct Protocol {
  
  /* out_bytes: Write data into this buffer. It's size is equal to *out_len
   *   out_len: Initially, *out_len tells you the size of the buffer out_bytes.
   *            However, it is also used as a return value, so you must set *out_len
   *            to the number of bytes you wrote to the buffer.
   *         x: the data you want to write to buffer
   */   	
  static bool Encode(uint8_t *out_bytes, uint32_t *out_len, const T &x) {
    return false;
  }
  
  /* in_bytes: Read data from this buffer. It's size is equal to *in_len
   *   in_len: Initially, *in_len tells you the size of the buffer in_bytes.
   *           However, it is also used as a return value, so you must set *in_len
   *           to the number of bytes you consume from the buffer.
   *        x: the data you want to read from the buffer
   */   
  static bool Decode(uint8_t *in_bytes, uint32_t *in_len, T &x) {
    return false;
  }
};

template <> struct Protocol<int> {
  static bool Encode(uint8_t *out_bytes, uint32_t *out_len, const int &x) {
	// check if buffer is big enough to fit the data, if not, return false
    if (*out_len < sizeof(int)) return false; 
	
	// do a memory copy of the data into the buffer, sizeof(int) is the size of the data
    memcpy(out_bytes, &x, sizeof(int));
	
	// since we wrote 4 bytes to the buffer, we set *out_len to 4
    *out_len = sizeof(int);

    return true;
  }
  
  static bool Decode(uint8_t *in_bytes, uint32_t *in_len, int &x) {
	// check if buffer is big enough to read in x, if not, return false
    if (*in_len < sizeof(int)) return false;
	
	// do a memory copy from the buffer into the data, sizeof(int) is the size of the data
    memcpy(&x, in_bytes, sizeof(int));
	
	// since we consumed 4 bytes from the buffer, we set *in_len to 4
    *in_len = sizeof(int);
	
    return true;
  }
};

// TASK2: Client-side
class IntParam : public BaseParams {
  int p;
 public:
  IntParam(int p) : p(p) {}

  bool Encode(uint8_t *out_bytes, uint32_t *out_len) const override {
    return Protocol<int>::Encode(out_bytes, out_len, p);
  }
};

// TASK2: Server-side
template <typename Svc>
class IntIntProcedure : public BaseProcedure {
  bool DecodeAndExecute(uint8_t *in_bytes, uint32_t *in_len,
                        uint8_t *out_bytes, uint32_t *out_len) override final {
    int x;
    // This function is similar to Decode. We need to return false if buffer
    // isn't large enough, or fatal error happens during parsing.
    if (!Protocol<int>::Decode(in_bytes, in_len, x)) {
      return false;
    }
    // Now we cast the function pointer func_ptr to its original type.
    //
    // This incomplete solution only works for this type of member functions.
    using FunctionPointerType = int (Svc::*)(int);
    auto p = func_ptr.To<FunctionPointerType>();
    int result = (((Svc *) instance)->*p)(x);
    if (!Protocol<int>::Encode(out_bytes, out_len, result)) {
      // out_len should always be large enough so this branch shouldn't be
      // taken. However just in case, we return a fatal error here.
      return false;
    }
    return true;
  }
};

// TASK2: Client-side
class IntResult : public BaseResult {
  int r;
 public:
  bool HandleResponse(uint8_t *in_bytes, uint32_t *in_len) override final {
    return Protocol<int>::Decode(in_bytes, in_len, r);
  }
  int &data() { return r; }
};

// TASK2: Client-side
class Client : public BaseClient {
 public:
  template <typename Svc>
  // Convert this into a variadic template method to support other method signatures.
  // As a starter though, you can try to overload this method multiple times
  // to support specific method signatures.
  IntResult *Call(Svc *svc, int (Svc::*func)(int), int x) {
    // Lookup instance and function IDs.
    int instance_id = svc->instance_id();
    int func_id = svc->LookupExportFunction(MemberFunctionPtr::From(func));

    // This incomplete solution only works for this type of member functions.
    // So the result must be an integer.
    auto result = new IntResult();

    // We also send the paramters of the functions. For this incomplete
    // solution, it must be one integer.
    if (!Send(instance_id, func_id, new IntParam(x), result)) {
      // Fail to send, then delete the result and return nullptr.
      delete result;
      return nullptr;
    }
    return result;
  }
};

// TASK2: Server-side
template <typename Svc>
class Service : public BaseService {
 protected:
  // Convert this into a template method to support other method signatures.
  // As a starter though, you can try to overload this method multiple times
  // to support specific method signatures.
  void Export(int (Svc::*func)(int)) {
    ExportRaw(MemberFunctionPtr::From(func), new IntIntProcedure<Svc>());
  }
};

}

#endif /* RPCXX_H */
