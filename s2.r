

setwd("/home/ss45797/cobrand_mre/")
library(tm)

library(stringr)

data2<- readRDS("data2.rds")
Data1<- readRDS("Data1.rds")

tags <- Data1[,c(1,2)]
tags[,1] <- tolower(tags[,1])
tags[,2] <- tolower(tags[,2])
tag_list <- unique(unlist(tags[,2])) 

doubt <- c(1:length(tag_list))
merchant_nm <- tag_list
head(tag_list,20)

dg <- 'step1'
dg


j = 1
for (tag in tag_list) 
{	
	shortlist <- tags[tags[,2] == tag,1]
	len <- length(shortlist)
	if(len == 1) 
	{ 
		doubt[j] = 1 
		merchant_nm[j] <- word(unlist(shortlist[1]),1)
	} 
	else 
	{ 
		doubt[j] = 0
		shortlist <- substr(shortlist,1,nchar(shortlist)-2)
		tags_corpus <- Corpus(VectorSource(unlist(shortlist)))
		TDM <- TermDocumentMatrix(tags_corpus,control = list(wordLengths = c(1, Inf)) )
		word_list <- findFreqTerms(TDM, len*0.8)
		if (length(word_list) > 1) 
		{ 
			position <- sapply(word_list,function(x) match(x, unlist(str_split(unlist(shortlist[1]),' '))))
			word_list <- word_list[order(position)]
		}
		key1 <- paste(word_list,collapse = ' ')
		key2 <- ""
		if(min(sapply( shortlist ,function(x) length(unlist(strsplit(unlist(x),' '))))) > 1) 
		{
			next_short <- shortlist[nchar(word(unlist(shortlist),1,2)) == min(nchar(word(unlist(shortlist),1,2)))]
			key2 <- word(unlist(next_short[1]),1,2)
		}
		merchant_nm[j] <- ifelse(nchar(key1) > nchar(key2) ,key1,key2) 
	}
	j = j +1
}

dg <- 'step2'
dg


tagtable <- cbind(unlist(tag_list),unlist(merchant_nm),unlist(doubt))
colnames(tagtable) <- c('docs4','mrechant_name','doubt')
tagtable[,'mrechant_name'] <- ifelse(nchar(tagtable[,'docs4']) > nchar(tagtable[,'mrechant_name']) ,tagtable[,'docs4'],tagtable[,'mrechant_name'])
head(tagtable)

Data1 <- as.data.frame(Data1)
keydata <- merge(tagtable,tags,by='docs4')
head(keydata)
head(Data1)
table(Data1$docs4)

dg <- 'step3'
dg


colnames(data2) <- c('description','merchant_name','key','b','clean description')
data2 <- data2[,c(1,2,3)]

colnames(keydata) <- c('key','merchant_name','a','description')
keydata <- keydata[,c(1,2,4)]

data2[,'key'] <- data2[,'merchant_name']

final_data <- rbind(data2,keydata)
final_data <- data.frame(lapply(final_data, as.character), stringsAsFactors=FALSE)

write.csv(final_data,"Cleaned.csv",row.names = FALSE)

saveRDS(final_data, "final_data.rds")

dg <- 'step4'
dg
