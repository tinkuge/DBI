#include <iostream>
#include <pthread.h>
#include <vector>
#include <algorithm>
#include <queue>
#include "BigQ.h"
#include "File.h"
#include "Pipe.h"
#include "DBFile.h"
//#include "Comparison.h"

using namespace std;

BigQ::BigQ(Pipe &inputPipe, Pipe &outputPipe, OrderMaker &sortOrder, int runLength){
    pagenum = 1;
    i = &inputPipe;
    o = &outputPipe;
    sord = &sortOrder;
    rLen = runLength;
    
    pthread_t worker;
    pthread_create(&worker, NULL, &sortRec, (void *) this);
}


void* BigQ::sortRec(void* b){
    DBFile *db = new DBFile();
    cout<<"Inside BigQ"<<endl;
    BigQ *bq = (BigQ*) b;
    File *f = new File();
    Record *r = new Record();
    Record *copy = new Record();
    Page *uPage = new Page();
    Page *sPage = new Page();
    vector<Record *> strRec;
    Pipe *in = bq-> i;
    Pipe *out = bq-> o;
    OrderMaker *om = bq->sord;
    int pnum = bq->pagenum;
    int runlen = bq->rLen;      //number of pages per run
    int numrun = 0;             //number of runs
    //TODO assign file name
    f->Open(0,"xyz");
    f->AddPage(uPage,0);
    //remove from the pipe  continuously
    while(in->Remove(r)){
        cout<<"removing a record from pipe"<<endl;
        //copy the current record because appending to the page will consume it
        copy->Copy(r);
        //when a record is removed, append it to current page
        if(uPage->Append(r)){
            //if successfully appended to the current page, put it in vector
            cout<<"push the record into the vector"<<endl;
            strRec.push_back(copy);
            //create new record object for copy
            copy  = new Record();
        }
        //if the page is full
        //if run is full
        else{
            //sort the vector
            sort(strRec.begin(), strRec.end(), Comparator(om));
            Record *i = new Record();
            pnum = 1;

            //Add sorted records to pages and write them to file
            for(auto i: strRec){
                //if the page has room, append it and do nothing
                if(sPage->Append(i)){
                    cout<<"Pushing sorted record into the page"<<endl;
                }

                //Otherwise write the page to the file
                else{
                    cout<<"Pushing sorted page into the file"<<endl;
                    f->AddPage(sPage,pnum-1);
                    sPage->EmptyItOut();
                    sPage = new Page();
                    pnum = pnum+1;
                    //if the next page is within runlength, add the record to the page
                    sPage->Append(i);
                }
            }
            numrun = numrun+1;
        }
            //After writing the existing sorted pages (run) to file , empty the pages and write current record to the empty page
        strRec.clear();
        uPage->EmptyItOut();
        sPage->EmptyItOut();
    }

    //sort and write last run
    sort(strRec.begin(), strRec.end(), Comparator(om));
    Record *i = new Record();
    pnum = 1;

    for(auto i: strRec){
        if(sPage->Append(i)){

        }

        else{
            f->AddPage(sPage,pnum-1);
            sPage->EmptyItOut();
            sPage = new Page();
            pnum = pnum+1;
            sPage->Append(i);
        }
    }
    //write out last page
    f->AddPage(sPage, pnum-1);
    //total number of runs
    numrun = numrun+1;
    strRec.clear();
    uPage->EmptyItOut();
    sPage->EmptyItOut();
    //close file and get number of pages
    int nump = f->GetLength();

    //merge - Phase 2
    //stores the first page of each run
    Page* runhead[numrun];
    int pagecounter[runlen];
    priority_queue<RunObj *, vector<RunObj *>, Comparator> priq(om); 
    Page* dummy = new Page();
    Record *dummyrec = new Record();
    if(nump != 0){
        //Get the first page of each run and store it in vector
        for(int i  = 1; i <= nump; i = i+runlen){
            f->GetPage(dummy, i-1);
            runhead[i] = dummy;
            pagecounter[i] = 1;
        }
        int j = 0;
        //get first record of each run
        for(auto i: runhead){
            j = j+1;
            
            RunObj *runob = new RunObj();
            if(i->GetFirst(dummyrec)){
                runob->ro = dummyrec;
                runob->rnum = j;
                priq.push(runob);
            }
            //dummyrec = NULL;
        }


        //start merge
        while(!priq.empty()){
            RunObj *pr = new RunObj();
            pr = priq.top();

            //nrun - current run
            int crun = pr->rnum;
            out->Insert(pr->ro);
            RunObj *npr = new RunObj();

            //If the page contains a record, fetch it and push it onto priority queue
            if(runhead[crun]->GetFirst(npr->ro)){
                //set run number for the fetched record object
                npr->rnum = crun;
                //push on to priority queue
                priq.push(npr);
            }

            //if the page is empty
            else{
                pagecounter[crun] = pagecounter[crun] + 1;
                if(pagecounter[crun] <= runlen){
                    int offset = (runlen*crun)+pagecounter[crun]+1;
                    f->GetPage(runhead[crun], offset-1);
                    runhead[crun]->GetFirst(npr->ro);
                    npr->rnum = crun;
                    priq.push(npr);
                }
            }
        }
    }
    f->Close();
    out->ShutDown();
}

