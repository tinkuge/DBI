#include <iostream>
#include <pthread.h>
#include <vector>
#include "File.h"
#include "Pipe.h"
class BigQ{
    private:
        char *fname;
        
        
    
    public:
        Pipe *i, *o;
        int pagenum = 0;
        OrderMaker *sord;
        int rLen;
        BigQ(Pipe &inputPipe, Pipe &outputPipe, OrderMaker &sortOrder, int runLength);
        static void* sortRec(void *);
};

class RunObj{
    public:
        Record *ro;
        int rnum;
        int cpage = 0;
};

class Comparator{
    public:
        OrderMaker *ordr;
        Comparator(OrderMaker *ord){
            ordr = ord;
        }
        bool operator()(RunObj *lr, RunObj *rr){
            ComparisonEngine ce;
            return ce.Compare(lr->ro,rr->ro,ordr) >= 0;
        }

        bool operator()(Record *lr, Record *rr){
            ComparisonEngine ce;
            return ce.Compare(lr,rr,ordr) >= 0;
        }
};