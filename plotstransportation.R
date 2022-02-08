#Ramadan Gannud
#2020 / 2021
# Transportation Networks and COVID19 - Chicago 

library("RColorBrewer")
library(ggplot2)
library(MASS)
library(gridExtra)
library(gganimate)
library(babynames)
library(hrbrthemes)
library(magrittr) # needs to be run every time you start R and want to use %>%
library(dplyr)    # alternatively, this also loads %>%
library(dplyr)
library(tidyverse)
library(gridExtra)
library(extrafont)
library(plyr)
library(data.table)
font_import()


#############################################################################################
#Airports

setwd('C:/Users/ramri/Desktop/Transportation_COVID19/datasets/results')

ds = read.table("daily_TNPAirport1.csv", header=T, sep=',')
head(ds)
nrow(ds)

dstx = read.table("daily_TaxiAirport1.csv", header=T, sep=',')
nrow(dstx)
head(dstx)
ds1 = merge(x = ds, y = dstx, all = TRUE)
ds1[is.na(ds1)] <- 0
nrow(ds1)
731*16
summary(ds1)

#dates
dates <- unique(ds1$date)
length(dates)

##
ds1$date <- as.Date(as.character(ds1$date), "%m/%d/%Y")
ds1$year <- format(as.Date(ds1$date, format="%Y-%m-%d"),"%Y")
ds1$day <- weekdays(as.Date(ds1$date))
ds1$month <- format(ds1$date,'%b')
head(ds1)
nrow(ds1)

#days
days <- unique(ds1[c("day")])
row.names(days) <- NULL
days <- days[days$day!='Monday',]
days <- append('Monday', days, after=5)

#months
months <- unique(ds1[c("month")])
row.names(months) <- NULL
mymonths <- c("Jan","Feb","Mar", "Apr","May","Jun", "Jul","Aug","Sep","Oct","Nov","Dec")



##################################################

#ds2 <- ds2[,c(5,4,8,1,9,7,6,2,12,11,10,3)]

#Pick up MidWay

#Change Parameters (PU,DO) (MDW,OH)
head(ds1)



par(mfrow = c(4, 3))
#Plot boundaries
lim <- 1.3*max(ds2$taxis)
#lim <- 300

##Months
ds2 <- ds1[ which( ds1$type == 'DO' & ds1$airport == 'MDW'),]
head(ds2)
nrow(ds2)

ds2 = ds2[,c("date","year","month","taxis")]
ds3 = setDT(ds2)[,.(taxis = sum(taxis)), by = c("date","year","month")]
ds3 = setDT(ds3)[,.(taxis = mean(taxis)), by = c("year","month")]
ds3 = spread(ds3, key='month' , value='taxis')
ds3 <- as.data.frame(ds3)
rownames(ds3) <- ds3[,1]
ds3 <- as.matrix(ds3[,-1])
ds3 <- ds3[,c(5,4,8,1,9,7,6,2,12,11,10,3)]

#lim <- 1.3*max(ds3)

ze_barplot <- barplot(ds3 , beside=T ,border="white",  font.lab=2,cex.axis = 1.5,cex.names = 1.3,
                      cex.lab=1.5, legend.text=F ,col=c("#86bbfd" , "#ff4d4d") , ylim=c(0,lim) ,font=3
)
text(x = ze_barplot , y = ds3 , label = round(ds3,0), pos = 3, cex = 1, col = "black",font=4)



##timeframes
ds2 <- ds1[ which( ds1$type == 'DO' & ds1$airport == 'MDW'),]
head(ds2)
nrow(ds2)

ds2 = ds2[,c("year","timeFrame","taxis")]
ds3 = setDT(ds2)[,.(taxis = mean(taxis)), by = c("year","timeFrame")]
ds3 = spread(ds3, key='timeFrame' , value='taxis')
ds3 <- as.data.frame(ds3)
rownames(ds3) <- ds3[,1]
ds3 <- as.matrix(ds3[,-1])
ds3 <- ds3[,c(1, 3, 2, 4)]
colnames(ds3) <- c("0 AM - 6 AM","6 AM - 12 PM","12 PM - 6 PM","6 PM - 0 AM")


#lim <- 1.3*max(ds3)

ze_barplot <- barplot(ds3 , beside=T ,border="white",  font.lab=2,cex.axis = 1.5,cex.names = 1.3,
                      cex.lab=1.5, legend.text=F ,col=c("#86bbfd" , "#ff4d4d") , ylim=c(0,lim) ,font=3
)
text(x = ze_barplot , y = ds3 , label = round(ds3,0), pos = 3, cex = 1, col = "black",font=4)






##days    (change taxis and trips)
ds2 <- ds1[ which( ds1$type == 'DO' & ds1$airport == 'MDW'),]
head(ds2)
nrow(ds2)

ds2 = ds2[,c("date","year","day","taxis")]
ds3 = setDT(ds2)[,.(taxis = sum(taxis)), by = c("date","year","day")]
ds3 = setDT(ds3)[,.(taxis = mean(taxis)), by = c("year","day")]
ds3 = spread(ds3, key='day' , value='taxis')
ds3 <- as.data.frame(ds3)
rownames(ds3) <- ds3[,1]
ds3 <- as.matrix(ds3[,-1])
ds3 <- ds3[,c(2, 6, 7, 5, 1, 3, 4)]

par(mfrow = c(1, 1))
#Plot boundaries
#lim <- 1.3*max(ds3)

ze_barplot <- barplot(ds3 , beside=T ,border="white",  font.lab=2,cex.axis = 1.5,cex.names = 1.3,
                      cex.lab=1.5, legend.text=F ,col=c("#86bbfd" , "#ff4d4d") , ylim=c(0,lim) ,font=3
)
text(x = ze_barplot , y = ds3 , label = round(ds3,0), pos = 3, cex = 1, col = "black",font=4)




###################################
#Multiplots


for (i in 1:12){
  ds3 <- ds2[ which( ds2$month== months[[1]][i]),]
  ds3 = ds3[c("year","timeFrame","trips")]
  ds3 = setDT(ds3)[,.(trips = mean(trips)), by = c("year","timeFrame")]
  ds3 = spread(ds3, key='timeFrame' , value='trips')
  ds3 <- as.data.frame(ds3)
  rownames(ds3) <- ds3[,1]
  ds3 <- as.matrix(ds3[,-1])
  ds3 <- ds3[,c(1, 3, 2, 4)]
  colnames(ds3) <- c("0-6AM","6AM-12PM","12-6PM","6PM-12AM")
  
  ze_barplot <- barplot(ds3 , beside=T ,border="white",  font.lab=2,cex.axis = 1.5,cex.names = 1.3,
                        cex.lab=1.5, legend.text=F ,col=c("#86bbfd" , "#ff4d4d") , ylim=c(0,lim) ,font=3
                        ,main =months[[1]][i]
  )
  text(x = ze_barplot , y = ds3 , label = round(ds3,0), pos = 3, cex = 1, col = "black",font=4)
  
}


for (j in 1:7){
  ds3 <- ds2[ which( ds2$day== days[[j]][1]),]
  ds3 = setDT(ds3)[,.(trips = mean(trips)), by = c("year","day")]
  ds3 = spread(ds3, key='day' , value='trips')
  ds3 <- as.data.frame(ds3)
  rownames(ds3) <- ds3[,1]
  ds3 <- as.matrix(ds3[,-1])
  ds3 <- ds3[,c(1, 3, 2, 4)]
  colnames(ds3) <- c("0-6AM","6AM-12PM","12-6PM","6PM-12AM")
  
  ze_barplot <- barplot(ds3 , beside=T ,border="white",  font.lab=2,cex.axis = 1.5,cex.names = 1.3,
                        cex.lab=1.5, legend.text=F ,col=c("#86bbfd" , "#ff4d4d") , ylim=c(0,lim) ,font=3
                        ,main =days[[1]][j]
  )
  text(x = ze_barplot , y = ds3 , label = round(ds3,0), pos = 3, cex = 1, col = "black",font=4)
  
}


par(mfrow = c(3, 4))
      
KK = 0
#for (i in 1:(length(months[[1]]))){
for (i in 1:10){  
  #for (j in 1:(length(days))){
  for (j in 1:4){
    print(paste(i,j,sep=","))
    ds3 <- ds2[ which( ds2$day==days[[j]][1] & ds2$month == months[[1]][i]),]
    ds3 = ds3[c("date","timeFrame","taxis")]
    ds3 = spread(ds3, key='timeFrame' , value='taxis')
    ds3[is.na(ds3)] <- 0
    ds3$date <- format(ds3$date,'%Y')
    ds3 <- aggregate(ds3[, 2:5], list(ds3$date), mean)
    rownames(ds3) <- ds3[,1]
    ds3 <- as.matrix(ds3[,-1])
    ds3 <- ds3[,c(1, 3, 2, 4)]
    
    #Plot boundaries
    lim <- 1.2*max(ds3)
    if (i == 1 & j == 1 ){
      ze_barplot <- barplot(ds3 , beside=T ,border="white", density =169,  font.lab=2,
                          cex.lab=1.5, legend.text=F,col=c("green" , "red") , ylim=c(0,lim) ,font=3
                          , main=days[[j]][1], ylab=months[[1]][i]
      )}
    else if (j == 1){ze_barplot <- barplot(ds3 , beside=T ,border="white", density =169,  font.lab=2,
                                           cex.lab=1.5, legend.text=F,col=c("green" , "red") , ylim=c(0,lim) ,font=3
                                           #, main=days[[j]][1]
                                           , ylab=months[[1]][i]
    )}
    else if ( i ==1){ze_barplot <- barplot(ds3 , beside=T ,border="white", density =169,  font.lab=2,
                                           cex.lab=1.5, legend.text=F,col=c("green" , "red") , ylim=c(0,lim) ,font=3
                                           , main=days[[j]][1]
                                           #, ylab=months[[1]][i]
    )}
    
    else {
      ze_barplot <- barplot(ds3 , beside=T ,border="white", density =200,  font.lab=2,
                            cex.lab=1.5, legend.text=F,col=c("green" , "red") , ylim=c(0,lim) ,font=3
                            #, main=days[[j]][1]
                            #, ylab=months[[1]][i]
      )
    }
    text(x = ze_barplot , y = ds3 , label = round(ds3,0), pos = 3, cex = .9, col = "black",font=4)
  
  # Use KK when plotting extra for three plots to four in a row
  KK = KK + 1
  
  #if (KK%%3 ==0){ze_barplot <- barplot(ds3 , beside=T ,border="white", density =200,  font.lab=2,
   #                                    cex.lab=1.5, legend.text=F,col=c("green" , "red") , ylim=c(0,lim) ,font=3
    #                                   , main='Ramaadan'
   #                                    #, ylab=months[[1]][i]
  #)   
  #text(x = ze_barplot , y = ds3 , label = round(ds3,0), pos = 3, cex = .9, col = "black",font=4)
  #}
  }

}


#################################################################################################
#Hospitals

hs = read.table("daily_TNPHospitals.csv", header=T, sep=',')

head(hs)
hs$date <- as.Date(as.character(hs$date), "%m/%d/%Y")
hs$year <- format(as.Date(hs$date, format="%m/%d/%Y"),"%Y")
hs$day <- weekdays(hs$date)
hs$month <- format(hs$date,'%b')
head(hs)
nrow(hs)
summary(hs)

#Take all days from TNP daily and append them to (Taxi to hospitals) to make up missing days
hsd = hs[c("date")]
#CHANGE HTrips to HTaxis
hs1 = read.table("daily_TaxiHospitals.csv", header=T, sep=',')
hs1$date <- as.Date(as.character(hs1$date), "%m/%d/%Y")
hs2 = merge(x = hsd, y = hs1, by = "date", all.x = TRUE)
hs2[is.na(hs2)] <- 0
hs2$year <- format(as.Date(hs2$date, format="%m/%d/%Y"),"%Y")
hs2$day <- weekdays(hs2$date)
hs2$month <- format(hs2$date,'%b')
nrow(hs2)
summary(hs2)


#Change HTrips and HTaxis, and change hs to hs2 when taxis
hs2 = hs2[,c("year","month","HTaxis")]
hs2 = setDT(hs2)[,.(HTaxis = mean(HTaxis)), by = c("year","month")]
hs2 = spread(hs2, key='month' , value='HTaxis')
hs2 <- as.data.frame(hs2)
rownames(hs2) <- hs2[,1]
hs2 <- as.matrix(hs2[,-1])
hs2 <- hs2[,c(5,4,8,1,9,7,6,2,12,11,10,3)]
hs2

par(mfrow = c(1, 1))


#Plot boundaries
lim <- 1.2*max(hs2)
ze_barplot <- barplot(hs2 , beside=T ,border="white",  font.lab=2,cex.axis = 1.2,cex.names = 1.2,
                      cex.lab=1.5, legend.text=F ,col=c("#86bbfd" , "#ff4d4d") , ylim=c(0,lim) ,font=3
                        )
text(x = ze_barplot , y = hs2 , label = round(hs2,0), pos = 3, cex = .8, col = "black",font=4)



####################################################################################################
#Crashes

######
#Daily

cs = read.table("daily_crashes.csv", header=T, sep=',')

head(cs)
unique(cs$date)
cs$date <- as.Date(as.character(cs$date), "%m/%d/%Y")
cs$year <- format(as.Date(cs$date, format="%m/%d/%Y"),"%Y")
cs$day <- weekdays(cs$date)
cs$month <- format(cs$date,'%b')
head(cs)
nrow(cs)

#Yearly crashes
cs_year = cs[c("year","crashes")]
cs_year = aggregate(cs_year[,2:2], list(cs_year$year), sum)
rownames(cs_year) <- cs_year[,1]
names(cs_year)[2] <- "crashes"
cs_year <- as.matrix(cs_year["crashes"])
cs_year


#Monthly crashes
cs_month = read.table("monthly_crashes.csv", header=T, sep=',')
cs_month$month <- mymonths[ cs_month$month ]
cs_month


par(mfrow = c(4, 3))
for (i in length(months[[1]])){
cs2 <- cs[ which( cs$month == months[[1]][1]),]
cs2 = cs2[c("date","day","crashes")]
cs2 = spread(cs2, key='day' , value='crashes')
cs2[is.na(cs2)] <- 0
cs2$date <- format(cs2$date,'%Y')
cs2 <- aggregate(cs2[, 2:8], list(cs2$date), mean)
rownames(cs2) <- cs2[,1]
cs2 <- as.matrix(cs2[,-1])
cs2 <- cs2[,c(2, 6, 7, 5, 1, 3, 4)]

#Plot boundaries
lim <- 1.2*max(cs2)
ze_barplot <- barplot(cs2 , beside=T ,border="white", density =100,  font.lab=2,
                      cex.lab=1.5, legend.text=T ,col=c("gray","green" , "red") , ylim=c(0,lim) ,font=3,main=months[[1]][i]
)
text(x = ze_barplot , y = cs2 , label = round(cs2,0), pos = 3, cex = .9, col = "black",font=4)

}

##########################################################
# Monthly

cs3 = cs[c("date","month","crashes")]
cs3 = spread(cs3, key='month' , value='crashes')
cs3[is.na(cs3)] <- 0
cs3$date <- format(cs3$date,'%Y')
cs3 <- aggregate(cs3[, 2:13], list(cs3$date), sum)
rownames(cs3) <- cs3[,1]
cs3 <- as.matrix(cs3[,-1])
cs3 <- cs3[,c(5,4,8,1,9,7,6,2,12,11,10,3)]

par(mfrow = c(1, 1))
#Plot boundaries
lim <- 1.3*max(cs3)
ze_barplot <- barplot(cs3 , beside=T ,border="white",  font.lab=2, cex.names = 1.2, cex.axis = 1.2,
                      cex.lab=1.5, legend.text=F ,col=c( "#005B44","#86bbfd" ,"#ff4d4d") ,
                      ylim=c(0,lim) ,font=3,main= "Monthly Car Crashes")

text(x = ze_barplot , y = cs3 , label = round(cs3,0), pos = 3, cex = .8, col = "black" ,font=4)


#00e6e6 #86bbfd
#eeff00
#5c5c5c #696969 
####################
#Monthly Hit and Run


#months vector assuming 1st month is Jan.
mymonths <- c("Jan","Feb","Mar", "Apr","May","Jun", "Jul","Aug","Sep","Oct","Nov","Dec")

csh = read.table("monthly_hit_and_run.csv", header=T, sep=',')

head(csh)
csh3 <- csh[ which( csh$hit_and_run == 'Y'),] 
csh3 <- csh3[,c(3,2,5)]
#add abbreviated month name
csh3$month <- mymonths[ csh3$month ]

csh3 = spread(csh3, key='month' , value='crashes_percentage')
rownames(csh3) <- csh3[,1]
csh3 <- as.matrix(csh3[,-1])
csh3 <- csh3[,c(5,4,8,1,9,7,6,2,12,11,10,3)]

par(mfrow = c(1, 1))
#Plot boundaries
lim <- 1.35*max(csh3)
ze_barplot <- barplot(csh3 , beside=T ,border="white",  font.lab=2, cex.names = 1.2, cex.axis = 1.2,
                      cex.lab=1.5, legend.text=F ,col=c("#005B44","#86bbfd" , "#ff4d4d") , ylim=c(0,lim) ,font=3,main= "Hit and Run"
)
text(x = ze_barplot , y = csh3 , label = sprintf("%% %.f",round(csh3,2)) , pos = 3, cex =1, col = "black",font=4)


####################
#street direction

css = read.table("street_direction.csv", header=T, sep=',')

head(css)
css3 <- css[,c(2,1,4)]

css3 = spread(css3, key='street_direction' , value='crashes_percentage')
rownames(css3) <- css3[,1]
css3 <- as.matrix(css3[,-1])
css3 <- css3[,c(2,1,3,4)]
colnames(css3) <- c("North","East","South","West")

par(mfrow = c(1, 1))
#Plot boundaries
lim <- 1.4*max(css3)
ze_barplot <- barplot(css3 , beside=T ,border="white",  font.lab=2,cex.names = 1.2, cex.axis = 1.2,
                      cex.lab=1.5, legend.text=F ,col=c("#005B44","#86bbfd" , "#ff4d4d") , ylim=c(0,lim) ,font=3,main= "Street Direction"
)
text(x = ze_barplot , y = css3 , label = sprintf("%% %.1f",round(css3,2)) , pos = 3, cex = 1, col = "black",font=4)

###########
#crash hour

ch = read.table("crash_hour.csv", header=T, sep=',')

head(ch)
ch3 <- ch[,c(2,1,4)]

ch3 = spread(ch3, key='crash_hour' , value='crashes_percentage')
rownames(ch3) <- ch3[,1]
ch3 <- as.matrix(ch3[,-1])
colnames(ch3) <- c("12 AM","1 AM","2 AM","3 AM","4 AM","5 AM","6 AM","7 AM","8 AM","9 AM","10 AM","11 AM","12 AM",
                   "1 PM","2 PM","3 PM","4 PM","5 PM","6 PM","7 PM","8 PM","9 PM","10 PM","11 PM")

par(mfrow = c(1, 1))
#Plot boundaries
lim <- 1.4*max(ch3)
ze_barplot <- barplot(ch3 , beside=T ,border="white",   font.lab=2,cex.names = 1.1, cex.axis = 1.2,
                      cex.lab=1.5, legend.text=T ,col=c("#005B44","#86bbfd" , "#ff4d4d") , ylim=c(0,lim) ,font=3,main= "Crash Hour"
)
text(x = ze_barplot , y = ch3 , label = sprintf("%.1f",round(ch3,2)) , pos = 3, cex = .8, col = "black",font=4)



#############
# Crashes day

csd = read.table("Crashes_day.csv", header=T, sep=',')

head(csd)
csd3 <- csd[,c(2,1,3)]

csd3 = spread(csd3, key='day' , value='crashes')
rownames(csd3) <- csd3[,1]
csd3 <- as.matrix(csd3[,-1])

par(mfrow = c(1, 1))
#Plot boundaries
lim <- 1.2*max(csd3)
ze_barplot <- barplot(csd3 , beside=T ,border="white", density =200,  font.lab=2,
                      cex.lab=1.5, legend.text=F ,col=c("gray","green" , "red") , ylim=c(0,lim) ,font=3,main= "Day of Month"
)
text(x = ze_barplot , y = csd3 , label = round(csd3,0), pos = 3, cex = .7, col = "black",font=4)



####################
#Lightning Condition

csl = read.table("lighting_condition.csv", header=T, sep=',')

head(csl)
csl3 <- csl[,c(2,1,4)]

csl3 = spread(csl3, key='lighting_condition' , value='crashes_percentage')
rownames(csl3) <- csl3[,1]
csl3 <- as.matrix(csl3[,-1])

par(mfrow = c(1, 1))
#Plot boundaries
lim <- 1.2*max(csl3)
ze_barplot <- barplot(csl3 , beside=T ,border="white", density =100,  font.lab=2,
                      cex.lab=1.5, legend.text=F ,col=c("gray","green" , "red") , ylim=c(0,lim) ,font=3,main= "Lightning Conditions"
)
text(x = ze_barplot , y = csl3 , label = sprintf("%% %.1f",round(csl3,2)) , pos = 3, cex = .7, col = "black",font=4)


########
#weekday


cw = read.table("crash_weekday.csv", header=T, sep=',')

head(cw)
cw3 <- cw[,c(2,1,4)]
cw3$crash_weekday <- days[ cw3$crash_weekday ]

cw3 = spread(cw3, key='crash_weekday' , value='crashes_percentage')
rownames(cw3) <- cw3[,1]
cw3 <- as.matrix(cw3[,-1])
cw3 <- cw3[,c(2,6,7,5,1,3,4)]

par(mfrow = c(1, 1))
#Plot boundaries
lim <- 1.2*max(cw3)
ze_barplot <- barplot(cw3 , beside=T ,border="white", density =100,  font.lab=2,
                      cex.lab=1.5, legend.text=F ,col=c("gray","green" , "red") , ylim=c(0,lim) ,font=3,main= "Week Day"
)
text(x = ze_barplot , y = cw3 , label = sprintf("%% %.1f",round(cw3,2)) , pos = 3, cex = .7, col = "black",font=4)




########################################################################################################
#TNP Trips

#cHANGE Taxi to TNP

ts = read.table("TNP_monthly.csv", header=T, sep=',')
head(ts)
ts3 <- ts[,c(1,2,10)]
ts3$month <- mymonths[ ts3$month ]


#CHANGE value and main (names(ts))

ts3 = spread(ts3, key='month' , value='Tippers_percentage')
rownames(ts3) <- ts3[,1]
ts3 <- as.matrix(ts3[,-1])
#ts3[is.na(ts3)] <- 0
ts3 <- ts3[,c(5,4,8,1,9,7,6,2,12,11,10,3)]

par(mfrow = c(1, 1))
#Plot boundaries
lim <- 1.25*max(ts3)
ze_barplot <- barplot(ts3 , beside=T ,border="white",  font.lab=2, cex.axis = 1.2,cex.names = 1.2,
                      cex.lab=1.5, legend.text= F,col=c("#86bbfd" , "#ff4d4d") , ylim=c(0,lim) ,font=3, 
                      main= "TNP Trips"
)
text(x = ze_barplot , y = ts3 , label = sprintf("%%%.f",round(ts3,2)) , pos = 3, cex = .9, col = "black",font=4)

#use when convert to million
text(x = ze_barplot , y = ts3 , label = paste(format(round(ts3 / 1e6, 1), trim = TRUE), "M") , pos = 3, cex = .9, col = "black",font=4)

#with dollar sign
#text(x = ze_barplot , y = ts3 , label = paste("$",paste(format(round(ts3 / 1e6, 1), trim = TRUE), "M")) , pos = 3, cex = .9, col = "black",font=4)




