#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/times.h> 
#include <sys/types.h>
#include <string.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

//struct to store records
struct records{
    int resID;
    char firstName[32];
    char lastName[32];
    int numDependents;
    float income;
    int postalCode;
};

void replace_multiple_space_with_one(char *s)
{
	int i=0, j=0;
	int count = 0;
	int len = strlen(s);

	// special case: if there is only one tab present, then replace 
	// that with a single blankspace
	if (len == 1 && s[0] == '\t') {
		s[0] = ' ';
		return;
	}

	// if length of the incoming string is < 2, then return
	if (len < 2)
		return;

	for (i=0 ; s[i] != '\0' ; i++) {
		if (s[i] == ' ' || s[i] == '\t') {
			count++;
		} 

		if (s[i] != ' ' && s[i] != '\t') {	
			if (count >= 1) {
				count = 0;
				s[j++] = ' ';
			}
			s[j++] = s[i];
		}
	}
	s[j] = '\0';

	return;
}

int main(int argc, char* argv[]){

    //calculate and store time stats
    double t1, t2, cpu_time,real_time;
    struct tms tb1, tb2;
    double ticspersec;
    int i, sum = 0;
    ticspersec = (double) sysconf(_SC_CLK_TCK);
    t1 = (double) times(&tb1);

    //read arguments
    char* filename=argv[0];
    int start=atoi(argv[1]);
    int stop=atoi(argv[2]);
    int field=atoi(argv[3]);
    char* order=argv[4];
    char* pipeName=argv[5];
    char* timePipe=argv[6];
    int rootID=atoi(argv[7]);

    //struct to read records
    struct records rawData[stop-start+1];
    FILE* dataFile;
    dataFile=fopen(filename,"r");

    //seek in file to the respective indices the sorter is supposed to read
    char* line = NULL;
    size_t nb2read = 0;
    ssize_t nbread;
    int count=0;
    for (int i=0;i<start;i++){
        getline(&line,&nb2read,dataFile);
        count++;
    }

    //read from the file
    for (int i=0;i<=stop-start;i++){
        if(getline(&line,&nb2read,dataFile)<0){
           printf("file reading error in bubble\n");
        }
        else{
            //normalize number of spaces between records
            replace_multiple_space_with_one(line);
            //store data in record struct array
            sscanf(line,"%d %s %s %d %f %d",&(rawData[i].resID),rawData[i].firstName,rawData[i].lastName,&(rawData[i].numDependents),&(rawData[i].income),&(rawData[i].postalCode));
        }
        
    }

    ///////////////////////////////
    //BUBBLE SORT TO SORT RECORDS//
    int j;
    struct records el;
    for (int i=0;i<stop-start;i++){
        for (int j=0;j<stop-start-i-1;j++){
            if (field==0){//check the attribute number to be used for comparison
                if (strcmp(order,"a")==0){//check the order of sorting
                    if (rawData[j].resID>rawData[j+1].resID){
                        struct records temp;
                        
                        temp.resID=rawData[j].resID;
                        strncpy(temp.firstName,rawData[j].firstName,32);
                        strncpy(temp.lastName,rawData[j].lastName,32);
                        temp.numDependents=rawData[j].numDependents;
                        temp.income=rawData[j].income;
                        temp.postalCode=rawData[j].postalCode;

                        rawData[j].resID=rawData[j+1].resID;
                        strncpy(rawData[j].firstName,rawData[j+1].firstName,32);
                        strncpy(rawData[j].lastName,rawData[j+1].lastName,32);
                        rawData[j].numDependents=rawData[j+1].numDependents;
                        rawData[j].income=rawData[j+1].income;
                        rawData[j].postalCode=rawData[j+1].postalCode;

                        rawData[j+1].resID=temp.resID;
                        strncpy(rawData[j+1].firstName,temp.firstName,32);
                        strncpy(rawData[j+1].lastName,temp.lastName,32);
                        rawData[j+1].numDependents=temp.numDependents;
                        rawData[j+1].income=temp.income;
                        rawData[j+1].postalCode=temp.postalCode;
                        
                    }
                }
                else if(strcmp(order,"d")==0){
                    if (rawData[j].resID<rawData[j+1].resID){
                        struct records temp;
                        
                        temp.resID=rawData[j].resID;
                        strncpy(temp.firstName,rawData[j].firstName,32);
                        strncpy(temp.lastName,rawData[j].lastName,32);
                        temp.numDependents=rawData[j].numDependents;
                        temp.income=rawData[j].income;
                        temp.postalCode=rawData[j].postalCode;

                        rawData[j].resID=rawData[j+1].resID;
                        strncpy(rawData[j].firstName,rawData[j+1].firstName,32);
                        strncpy(rawData[j].lastName,rawData[j+1].lastName,32);
                        rawData[j].numDependents=rawData[j+1].numDependents;
                        rawData[j].income=rawData[j+1].income;
                        rawData[j].postalCode=rawData[j+1].postalCode;

                        rawData[j+1].resID=temp.resID;
                        strncpy(rawData[j+1].firstName,temp.firstName,32);
                        strncpy(rawData[j+1].lastName,temp.lastName,32);
                        rawData[j+1].numDependents=temp.numDependents;
                        rawData[j+1].income=temp.income;
                        rawData[j+1].postalCode=temp.postalCode;
                        
                    }
                }
            }
            else if (field==3)
            {
                if (strcmp(order,"a")==0){
                    if (rawData[j].numDependents>rawData[j+1].numDependents){
                        struct records temp;
                        
                        temp.resID=rawData[j].resID;
                        strncpy(temp.firstName,rawData[j].firstName,32);
                        strncpy(temp.lastName,rawData[j].lastName,32);
                        temp.numDependents=rawData[j].numDependents;
                        temp.income=rawData[j].income;
                        temp.postalCode=rawData[j].postalCode;

                        rawData[j].resID=rawData[j+1].resID;
                        strncpy(rawData[j].firstName,rawData[j+1].firstName,32);
                        strncpy(rawData[j].lastName,rawData[j+1].lastName,32);
                        rawData[j].numDependents=rawData[j+1].numDependents;
                        rawData[j].income=rawData[j+1].income;
                        rawData[j].postalCode=rawData[j+1].postalCode;

                        rawData[j+1].resID=temp.resID;
                        strncpy(rawData[j+1].firstName,temp.firstName,32);
                        strncpy(rawData[j+1].lastName,temp.lastName,32);
                        rawData[j+1].numDependents=temp.numDependents;
                        rawData[j+1].income=temp.income;
                        rawData[j+1].postalCode=temp.postalCode;
                        
                    }
                }
                else if(strcmp(order,"d")==0){
                    if (rawData[j].numDependents<rawData[j+1].numDependents){
                        struct records temp;
                        
                        temp.resID=rawData[j].resID;
                        strncpy(temp.firstName,rawData[j].firstName,32);
                        strncpy(temp.lastName,rawData[j].lastName,32);
                        temp.numDependents=rawData[j].numDependents;
                        temp.income=rawData[j].income;
                        temp.postalCode=rawData[j].postalCode;

                        rawData[j].resID=rawData[j+1].resID;
                        strncpy(rawData[j].firstName,rawData[j+1].firstName,32);
                        strncpy(rawData[j].lastName,rawData[j+1].lastName,32);
                        rawData[j].numDependents=rawData[j+1].numDependents;
                        rawData[j].income=rawData[j+1].income;
                        rawData[j].postalCode=rawData[j+1].postalCode;

                        rawData[j+1].resID=temp.resID;
                        strncpy(rawData[j+1].firstName,temp.firstName,32);
                        strncpy(rawData[j+1].lastName,temp.lastName,32);
                        rawData[j+1].numDependents=temp.numDependents;
                        rawData[j+1].income=temp.income;
                        rawData[j+1].postalCode=temp.postalCode;
                        
                    }
                }
            }
            else if (field==4)
            {
                if (strcmp(order,"a")==0){
                    if (rawData[j].income>rawData[j+1].income){
                        struct records temp;
                        
                        temp.resID=rawData[j].resID;
                        strncpy(temp.firstName,rawData[j].firstName,32);
                        strncpy(temp.lastName,rawData[j].lastName,32);
                        temp.numDependents=rawData[j].numDependents;
                        temp.income=rawData[j].income;
                        temp.postalCode=rawData[j].postalCode;

                        rawData[j].resID=rawData[j+1].resID;
                        strncpy(rawData[j].firstName,rawData[j+1].firstName,32);
                        strncpy(rawData[j].lastName,rawData[j+1].lastName,32);
                        rawData[j].numDependents=rawData[j+1].numDependents;
                        rawData[j].income=rawData[j+1].income;
                        rawData[j].postalCode=rawData[j+1].postalCode;

                        rawData[j+1].resID=temp.resID;
                        strncpy(rawData[j+1].firstName,temp.firstName,32);
                        strncpy(rawData[j+1].lastName,temp.lastName,32);
                        rawData[j+1].numDependents=temp.numDependents;
                        rawData[j+1].income=temp.income;
                        rawData[j+1].postalCode=temp.postalCode;
                        
                    }
                }
                else if(strcmp(order,"d")==0){
                    if (rawData[j].income<rawData[j+1].income){
                        struct records temp;
                        
                        temp.resID=rawData[j].resID;
                        strncpy(temp.firstName,rawData[j].firstName,32);
                        strncpy(temp.lastName,rawData[j].lastName,32);
                        temp.numDependents=rawData[j].numDependents;
                        temp.income=rawData[j].income;
                        temp.postalCode=rawData[j].postalCode;

                        rawData[j].resID=rawData[j+1].resID;
                        strncpy(rawData[j].firstName,rawData[j+1].firstName,32);
                        strncpy(rawData[j].lastName,rawData[j+1].lastName,32);
                        rawData[j].numDependents=rawData[j+1].numDependents;
                        rawData[j].income=rawData[j+1].income;
                        rawData[j].postalCode=rawData[j+1].postalCode;

                        rawData[j+1].resID=temp.resID;
                        strncpy(rawData[j+1].firstName,temp.firstName,32);
                        strncpy(rawData[j+1].lastName,temp.lastName,32);
                        rawData[j+1].numDependents=temp.numDependents;
                        rawData[j+1].income=temp.income;
                        rawData[j+1].postalCode=temp.postalCode;
                        
                    }
                }
            }
            else if (field==5)
            {
                if (strcmp(order,"a")==0){
                    if (rawData[j].postalCode>rawData[j+1].postalCode){
                        struct records temp;
                        
                        temp.resID=rawData[j].resID;
                        strncpy(temp.firstName,rawData[j].firstName,32);
                        strncpy(temp.lastName,rawData[j].lastName,32);
                        temp.numDependents=rawData[j].numDependents;
                        temp.income=rawData[j].income;
                        temp.postalCode=rawData[j].postalCode;

                        rawData[j].resID=rawData[j+1].resID;
                        strncpy(rawData[j].firstName,rawData[j+1].firstName,32);
                        strncpy(rawData[j].lastName,rawData[j+1].lastName,32);
                        rawData[j].numDependents=rawData[j+1].numDependents;
                        rawData[j].income=rawData[j+1].income;
                        rawData[j].postalCode=rawData[j+1].postalCode;

                        rawData[j+1].resID=temp.resID;
                        strncpy(rawData[j+1].firstName,temp.firstName,32);
                        strncpy(rawData[j+1].lastName,temp.lastName,32);
                        rawData[j+1].numDependents=temp.numDependents;
                        rawData[j+1].income=temp.income;
                        rawData[j+1].postalCode=temp.postalCode;
                        
                    }
                }
                else if(strcmp(order,"d")==0){
                    if (rawData[j].postalCode<rawData[j+1].postalCode){
                       struct records temp;
                        
                        temp.resID=rawData[j].resID;
                        strncpy(temp.firstName,rawData[j].firstName,32);
                        strncpy(temp.lastName,rawData[j].lastName,32);
                        temp.numDependents=rawData[j].numDependents;
                        temp.income=rawData[j].income;
                        temp.postalCode=rawData[j].postalCode;

                        rawData[j].resID=rawData[j+1].resID;
                        strncpy(rawData[j].firstName,rawData[j+1].firstName,32);
                        strncpy(rawData[j].lastName,rawData[j+1].lastName,32);
                        rawData[j].numDependents=rawData[j+1].numDependents;
                        rawData[j].income=rawData[j+1].income;
                        rawData[j].postalCode=rawData[j+1].postalCode;

                        rawData[j+1].resID=temp.resID;
                        strncpy(rawData[j+1].firstName,temp.firstName,32);
                        strncpy(rawData[j+1].lastName,temp.lastName,32);
                        rawData[j+1].numDependents=temp.numDependents;
                        rawData[j+1].income=temp.income;
                        rawData[j+1].postalCode=temp.postalCode;
                        
                    }
                }
            }
              
        }
    }

    //end of sorter time stats
    t2 = (double) times(&tb2);
    cpu_time = (double) ((tb2.tms_utime + tb2.tms_stime) -(tb1.tms_utime + tb1.tms_stime));
    cpu_time/=ticspersec;
    real_time=(t2 - t1) / ticspersec;

    //pass time stats via named pipe to merger
    int td=open(timePipe,O_WRONLY);
    struct timeData{
        double cpu_time;
        double real_time;
    };
    struct timeData timeStorage;
    timeStorage.cpu_time=cpu_time;
    timeStorage.real_time=real_time;
    write(td,&timeStorage,sizeof(struct timeData));
    close(td);

    //pass sorted records in sorter's range to merger
    int rangeWorked=stop-start;
    int fd=open(pipeName,O_WRONLY);
    for (int i=0;i<rangeWorked+1;i++){
        write(fd,&rawData[i],sizeof(struct records));
    }
    close(fd);
    
    //send SIGUSR1 signal to root
    kill(rootID, SIGUSR1);
    //exit sorter
    exit(0);
}