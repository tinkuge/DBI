#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include<iostream>
#include<string.h>

// stub file .. replace it with your own DBFile.cc
using namespace std;
DBFile::DBFile () {
    pdirty = false;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    char *varf  = strdup(f_path);
    f.Open(0,varf);
    pnum = 1;
    return 1;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    
    fload = fopen(loadpath, "r");
    if(fload == nullptr){
        cout<<"File wasn't able to be opened"<<endl;
    }
     
    while(true){
         int ret = r.SuckNextRecord(&f_schema,fload);
         if(ret == 1){
            Add(r);
         }
         else
         {
             break;
         }
         

    }




}

int DBFile::Open (const char *f_path) {
    char *fp = strdup(f_path);
    f.Open(1,fp);
    pnum = 1;
    return 1;
}

void DBFile::MoveFirst () {
    f.GetPage(&p,1);
    pnum = 1;
}

int DBFile::Close () {
    if(pdirty == true){
        f.AddPage(&p,pnum);
    }
}

void DBFile::Add (Record &rec) {
    int ret = p.Append(&rec);
    if(ret == 1){
        pdirty = true;
        cout<<"Record appended";
    }
    else{
        cout<<"Page full";
        f.AddPage(&p,pnum);
        p.EmptyItOut();
        pdirty = false;
        pnum = pnum + 1;
        p.Append(&rec);
        pdirty = true;
    }
}

int DBFile::GetNext (Record &fetchme) {
    int ret = p.GetFirst(&fetchme);
    
    if(ret != 0){
        return ret;
    }
    else{
        p.EmptyItOut();
        
        if(pnum < f.GetLength()){
            f.GetPage(&p,pnum);
            pnum = pnum + 1;
            //store return value of new page in newret
            int newret = p.GetFirst(&fetchme);
            if(newret!=0){
                return newret;
            }
            //New page's first record is empty
            else{
                return 0;
            }

        }
        //if no more pages exist, after the last page is emptied
        else{
            return 0;
        }
    }
    
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    while(true){
        int retv = GetNext(fetchme);
        if(retv == 1){
            int r = ce.Compare(&fetchme,&literal,&cnf);
            if(r!=0){
                return 1;
            }
            else{
                return 0;
            }
        }

        else{
            return 0;
        }
    }
}
