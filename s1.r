

setwd("/home/ss45797/cobrand_mre/")
library(tm)

train <- read.csv("data_r.csv",header=FALSE)

colnames(train) <- c('MERCH_DSC','TXN','SALES')

head(train,20)

description <- train$MERCH_DSC

dt<- as.matrix(description)
docs4<- dt

num_char <- 1:length(dt)
Data1<-cbind(dt,docs4,num_char,num_char,docs4)
Data1[,3] <- 7
Data1[,4] <- 0
head(Data1)

dg <- 'step1'
dg


library(stringr)
red <- read.csv("Redundant_words.csv")
first_redundancy <- red$first_redundancy
first_redundancy <- as.character(first_redundancy )
len_redun <- nchar(first_redundancy)
len_redun1 <- ifelse(len_redun < 7,7,len_redun) 
delta <- 4 
num_red <- length(first_redundancy )

for (i in 1:num_red) {
for (j in 1:nrow(Data1)) {
if(substr(Data1[j,2],1,len_redun[i]) == first_redundancy[i] & as.numeric(Data1[j,3]) < len_redun1[i] + delta ){ 
 Data1[j,3] <- len_redun1[i] + delta  }
}
}

for (j in 1:nrow(Data1)) {
Data1[j,2] = substr(Data1[j,2],1,as.numeric(Data1[j,3]))
}

for (i in 1:nrow(Data1)) {
if(grepl("WWW",Data1[i,1]) == 1 &  grepl("CO",Data1[i,1]) == 1){
start <- str_locate(unlist(Data1[i,1]),"WWW")[2]
end <- str_locate(unlist(Data1[i,1]),"CO")[1]
Data1[i,2]  <- paste("www",tolower(substr(unlist(Data1[i,1]),start + 1,end-1)),"com", sep = ".")
Data1[i,4]  <- 1}
}

table(unlist(Data1[,4]))[2]/nrow(Data1)

dg <- 'step2'
dg

rmr2, foreach/parfor


ss <- read.csv("sure_shots.csv")
sure_shots <- as.character(ss$Keywords)
sure_tags <- as.character(ss$Merchant_Name)

for (i in 1:length(sure_shots)) {
for (j in 1:nrow(Data1)) {
if(grepl(sure_shots[i],Data1[j,5]) == 1){Data1[j,2] <- sure_tags[i]
Data1[j,4] <- 1
}
}
}


table(unlist(Data1[,4]))[2]/nrow(Data1)

dg <- 'step3'
dg


ssf <- read.csv("first_words.csv")
sure_shots_fw <- as.character(ssf$Keywords)
sure_tags_fw <- as.character(ssf$Merchant_Name)

for (i in 1:length(sure_shots_fw)) {
for (j in 1:nrow(Data1)) {
if(word(unlist(Data1[j,5]),1) == sure_shots_fw[i]){Data1[j,2] <- sure_tags_fw[i]
Data1[j,4] <- 1
}
}
}


table(unlist(Data1[,4]))[2]/nrow(Data1)

dg <- 'step4'
dg

data2 <- Data1[Data1[,4] == 1,]
nrow(data2)/nrow(Data1)
Data1 <- Data1[Data1[,4] != 1,]

head(data2,100)
head(Data1,100)

saveRDS(data2, "data2.rds")
saveRDS(Data1, "Data1.rds")

dg <- 'step5'
dg
