// Author: Jishan Baig

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include<string>
#include<iostream>
#include<fstream>
#include<sstream>
#include<vector>
#include<iterator>
#include<algorithm>


#define prepared 0
#define prepare 1
#define commit 2
 
using namespace std;

void tocomm(string request,int fn)
{
int reqline,b,i;
stringstream fni;
stringstream fno;
fni<<"store"<<fn<<".txt";
fno<<"res"<<fn<<".txt";
string ipfilename=fni.str(); 
string opfilename=fno.str();
ifstream infile(ipfilename.c_str());
ofstream outfile(opfilename.c_str());
stringstream is;
string line,trans;
string word1,word2,act_str;
vector<string> v,tokens;
vector<string>::iterator it;
istringstream iss(request);
copy(istream_iterator<string>(iss),istream_iterator<string>(),back_inserter(tokens));
for(it=tokens.begin();it!=tokens.end();it++)  
cout<<*it<<" "<<endl;

for(int lineNum = 0; getline(infile, line); lineNum++)
{
    stringstream ss(line);
    string word;
    ss>>word; 
    if(word.compare(tokens[0])==0)
      {
         reqline=lineNum;
         word1=word;;
         ss >> word;
         word2=word;
         if(tokens[1].compare("add")==0) 
            b = atoi(word2.c_str())+atoi(tokens[2].c_str());
         else
            b = atoi(word2.c_str())-atoi(tokens[2].c_str());
         is<<b;
         act_str=word1+' '+is.str();
         //cout<<act_str<<endl;
         v.push_back(act_str);
       }
     else
       v.push_back(line);
}
printf("line to be changed : %d\n",reqline);
for(i=0;i<v.size();i++)
outfile<<v[i]<<endl;
outfile.close();
}


int main(int argc, char** argv) {
 //string result;
  // Initialize the MPI environment
  MPI_Init(NULL, NULL);
  // Find out rank, size
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  // We are assuming at least 2 processes for this task
  if (world_size < 3) {
    fprintf(stderr, "World size must be greater than 2 for %s\n", argv[0]);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  int number=1;
  //printf("%d\n",world_rank);
  if (world_rank == 0) 
  {
    printf("I am the Co-ordinator\n");
    int stats[2];
    //receiving prepared from requsting node 2.
    //MPI_Recv(&number, 1, MPI_INT, 2, prepared, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    //receiving a string starts from here.
    MPI_Status status;
    MPI_Probe(2,prepared,MPI_COMM_WORLD, &status);
    int l;// = status.Get_count(MPI_CHAR);
    MPI_Get_count(&status, MPI_CHAR, &l);
    char *buf = new char[l];
    MPI_Recv(buf, l, MPI_CHAR, 2, prepared, MPI_COMM_WORLD, &status);
    string truestr(buf, l);
    printf("Process 0 received prepared from process 2 with transaction :: %s\n",truestr.c_str());
    //receiving a string ends here.
    delete [] buf;
    stats[1]=number;
    //sending to prepare to all other processes i.e. 1.
    MPI_Send(&number, 1, MPI_INT, 1, prepare, MPI_COMM_WORLD);
    printf("Process 0 sent prepare to process 1\n");
    MPI_Recv(&number, 1, MPI_INT, 1, prepared, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    printf("Process 0 received prepared from process 1\n");
    stats[0]=number;
    if(stats[0]==1 && stats[1]==1)
     {
   
       MPI_Send(truestr.c_str(), truestr.size(), MPI_CHAR, 1, commit, MPI_COMM_WORLD);
       printf("Process 0 sent commit to process 1\n");
       MPI_Send(truestr.c_str(), truestr.size(), MPI_CHAR, 2, commit, MPI_COMM_WORLD);
       printf("Process 0 sent commit to process 2\n");
     }
     else
     {
       truestr="0";
       MPI_Send(truestr.c_str(), truestr.size(), MPI_CHAR, 1, commit, MPI_COMM_WORLD);
       printf("Process 0 sent abort to process 1\n");
       MPI_Send(truestr.c_str(), truestr.size(), MPI_CHAR, 2, commit, MPI_COMM_WORLD);
       printf("Process 0 sent abort to process 2\n");
     }
     printf("Process 0 has completed its task\n");
  } 

  else if (world_rank == 1) 
  {
    printf("I am the Resource Manager-1\n");
    MPI_Recv(&number, 1, MPI_INT, 0, prepare, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    printf("Process 1 received prepare from process 0\n");
    //Process 1 can send either number 0(not prep) or number 1(prep).
    number=1; //number=1;
    MPI_Send(&number, 1, MPI_INT, 0, prepared, MPI_COMM_WORLD);
    printf("Process 1 sent prepared to process 0\n");
    //process of receiving starts.
    MPI_Status status;
    MPI_Probe(0, commit,MPI_COMM_WORLD, &status);
    int l;// = status.Get_count(MPI_CHAR);
    MPI_Get_count(&status, MPI_CHAR, &l);
    char *buf = new char[l];
    MPI_Recv(buf, l, MPI_CHAR, 0, commit, MPI_COMM_WORLD, &status);
    string getby_1(buf, l);
    delete [] buf;
    //process of receiving ends.
    //do commit/abort
    if(getby_1.size()>1)
      {
      printf("Process 1 received string %s from process 0\n",getby_1.c_str());       
      printf("Process 1 has committed\n");
      tocomm(getby_1,world_rank);
      }
    else
      printf("Process 1 has aborted\n");
  }
 else //world_rank==2
  {
   printf("I am the Resource Manager-2\n");  
   //take transaction
   printf("Enter the transaction you want to perform : ");
   string trans="jishan add 200";
   //getline(cin,trans);
   //cout<<trans<<endl;
   //printf("hi");
   //sending prepared to co-ordinator.
   //MPI_Send(&number, 1, MPI_INT, 0, prepared, MPI_COMM_WORLD);
   MPI_Send(trans.c_str(), trans.size(), MPI_CHAR, 0, prepared, MPI_COMM_WORLD);
   printf("Process 2 sent prepared with transaction detail to process 0\n");
   //process of receiving starts.
    MPI_Status status;
    MPI_Probe(0, commit,MPI_COMM_WORLD, &status);
    int l;// = status.Get_count(MPI_CHAR);
    MPI_Get_count(&status, MPI_CHAR, &l);
    char *buf = new char[l];
    MPI_Recv(buf, l, MPI_CHAR, 0, commit, MPI_COMM_WORLD, &status);
    string getby_2(buf, l);
    delete [] buf;
    //process of receiving ends.
   //do commit/abort
   if(getby_2.size()>1)
      {
      printf("Process 2 received string %s from process 0\n",getby_2.c_str());
      printf("Process 2 has committed\n");
      tocomm(getby_2,world_rank);
      }
    else
      printf("Process 2 has aborted the transaction\n");
  }
  
  MPI_Finalize();
}






