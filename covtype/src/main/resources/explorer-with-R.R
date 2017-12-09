install.packages("caret")
install.packages("plyr")
install.packages("dplyr")
install.packages("tidyr")
install.packages("ggplot2")

library(caret)
library(plyr)
library(dplyr)
library(tidyr)
library(ggplot2)

df <- read.csv("/tmp/covtype.csv")
str(df)
sapply(df,function(x)any(is.na(x)))
set.seed(42)
limit.rows <- 250000
df <- df[sample(nrow(df),limit.rows),]
table(df$Cover_Type)

df <- df %>%
  gather(key=Region, value=region.indicator,Wilderness_Area1:Wilderness_Area4)%>%
  filter(region.indicator==1) %>%
  select(-region.indicator)
df$Region <- ifelse(df$Region=="Wilderness_Area1","Rawah",
                        ifelse(df$Region=="Wilderness_Area2","Neota",
                        ifelse(df$Region=="Wilderness_Area3","Comanche Peak", 
                               "Cache la Poudre")))
df$Region <- as.factor(df$Region)
df$Cover_Type <- as.character(df$Cover_Type)
df$Cover_Type <- ifelse(df$Cover_Type==1,"Spruce/Fir",
                        ifelse(df$Cover_Type==2,"Lodgepole Pine",
                        ifelse(df$Cover_Type==3,"Ponderosa Pine",
                        ifelse(df$Cover_Type==4,"Cottonwood/Willow ",
                        ifelse(df$Cover_Type==5,"Aspen ",
                        ifelse(df$Cover_Type==6,"Douglas-fir ",
                                        "Krummholz"))))))
df <- df %>%
  gather(key=Soil, value=soil.indicator,Soil_Type1:Soil_Type40)%>%
  filter(soil.indicator==1) %>%
  select(-soil.indicator)
df$Cover_Type <- as.factor(df$Cover_Type)

ggplot(data=df) +
  geom_bar(aes(x=Cover_Type,fill=Cover_Type),color="black") + 
  facet_wrap(~Soil,scale="free") +
  theme_bw() +
  xlab("Count") + 
  ylab("Tipo de Cobertura Vegetal") + 
  ggtitle("Tipo de Cobertura Vegetal versus Tipo de Solo")+
  theme(axis.text=element_blank()) + 
  theme(legend.position= "bottom")
  
ggplot(data=df) +
  geom_bar(aes(x=Cover_Type),fill="#66CC99",color="black") + 
  facet_wrap(~Region) +
  coord_flip() +
  theme_bw() +
  xlab("Quantidade") + 
  ylab("Tipo de Cobertura Vegetal") + 
  ggtitle("Tipo de Cobertura Vegetal versus Regiao")

ggplot(data=df) +
  geom_point(aes(x=Slope,
                 y=Elevation,
                 color=Cover_Type
  ),alpha=0.5) + 
  theme_bw() + 
  ylab("Elevation") +
  xlab("Slope") +
  guides(color = guide_legend(title = "Cover Type")) + 
  theme(legend.position= "bottom") + 
  scale_color_brewer(palette = "Set3")+ 
  ggtitle("Inclinacao versus Elevacao")
