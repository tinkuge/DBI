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
    file = new File();
    p = new Page();
    pdirty = false;
    pnum =1;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    char *varf  = strdup(f_path);
    file->Open(0,varf);
  //  pnum = 1;
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
    file->Open(1,fp);
    pnum = 1;
    cout << "open" << endl;
    return 1;
}

void DBFile::MoveFirst () {
    cout << "move" << endl;
    cout <<"move 1" << endl;
    file->GetPage(p,0);
    cout << "1" << endl;
    //cout << "3" << endl;
    pnum = 1;
    //cout << "2" << endl;

}

int DBFile::Close () {
    if(pdirty == true){
        file->AddPage(p,pnum);
        p->EmptyItOut();
    }
    file->Close();

}

void DBFile::Add (Record &rec) {
    int ret = p->Append(&rec);
    if(ret == 1){
        pdirty = true;
        cout<<"Record appended";
    }
    else{
        cout<<"Page full";
        file->AddPage(p,pnum);
        p->EmptyItOut();
        pdirty = false;
        pnum = pnum + 1;
        p->Append(&rec);
        pdirty = true;
    }
}

int DBFile::GetNext (Record &fetchme) {
  //  cout << "11" << endl;
    int ret = p->GetFirst(&fetchme);
    // cout << "322" << endl;
    // cout << file->GetLength() << endl;
    // cout << ret << endl;

    if(ret != 0){
        return ret;
    }
    else{
        p->EmptyItOut();
        if(pnum < file->GetLength()-1){
            file->GetPage(p,pnum);
            pnum = pnum + 1;
            //store return value of new page in newret
            int newret = p->GetFirst(&fetchme);
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
  ComparisonEngine ce;
    while(GetNext(fetchme)){
        if(ce.Compare(&fetchme,&literal,&cnf)){
            return 1;
    }
}
return 0;
}
