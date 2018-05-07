# -*- coding: utf-8 -*-
"""
Created on Tue Mar 20 12:13:57 2018

@author: Matthieu Salor.
"""

import pandas as pd
import numpy as np
import math

def calculPerf(histoPTF, histoPrice):
    histoRTN = histoPrice / histoPrice.shift(1)
    histoRTN.fillna(1,inplace = True)
    # On suppose que le portefeuille est rebalancé à l'ouverture de la date du fichier histoPTF
    valuePTF = 100
    # i in range exclut la bordure supérieur [inf;sup[
    for i in range(0, len(histoPTF.index)):
        
        myPTF = histoPTF.iloc[[i]] 
        
        #On prend l'historique de Return entre notre rebalancement et le prochain
        #S'il n'y a pas d'autre rebalancement, on prend l'historique entre notre rebalancement et le dernier return disponible.
        if i == len(histoPTF.index)-1:            
            histoRTNtemp = histoRTN[histoRTN.index.get_loc(histoPTF.index[i].date(), method='backfill')+1:]
        else:
            histoRTNtemp = histoRTN[histoRTN.index.get_loc(histoPTF.index[i].date(), method='backfill')+1:histoRTN.index.get_loc(histoPTF.index[i+1].date(),method='backfill')+1]
        
        #On calcul la valeur du portefeuille en multipliant les poids par la somme du produit des (rendements + 1)
        valuePTF = (valuePTF + valuePTF * (myPTF*(histoRTNtemp.product(0)-1)).sum(1))[0]

        #S'il s'agit de la première itération, on initialise notre histoValuePtf
        if i == 0:
            histoValuePtf = pd.DataFrame({'Value': [valuePTF], 'Date': [histoRTNtemp.index[len(histoRTNtemp.index)-1]]})
        #Sinon on la concatène avec la nouvelle valeur.
        else:
            frame = [histoValuePtf,pd.DataFrame({'Value': [valuePTF], 'Date': [histoRTNtemp.index[len(histoRTNtemp.index)-1]]})]
            histoValuePtf = pd.concat(frame)
            
        #On supprime  la variable pour la réinitialiser
        del histoRTNtemp
    
    #On définit la date comme index
    histoValuePtf.set_index('Date', inplace=True)
    return histoValuePtf

# Stratégie consistant à donner 1 point pour chaque performance positive sur les 3 périodes définies.
# Les poids définis sont la somme des points de chaque action divisé par la somme des points totaux.
def priceMomentumAnalysis(histoPriceV):
    # On itère dans notre histoPrice

    firstMM = 1
    secondMM = 2
    lastMM = 3  
 
    monthlyHistoPrice = histoPriceV.iloc[histoPriceV.reset_index().groupby(histoPriceV.index.to_period('W'))['index'].idxmax()]
    # On va créer un dataframe perf 1 mois futur et perf 1 mois passé
    firstMomentum = monthlyHistoPrice / monthlyHistoPrice.shift(firstMM) -1
    secondMomentum = monthlyHistoPrice / monthlyHistoPrice.shift(secondMM) -1
    lastMomentum = monthlyHistoPrice / monthlyHistoPrice.shift(lastMM) -1
       
    firstMomentum[(firstMomentum > 0)] = 1
    firstMomentum[(firstMomentum < 0)] = 0
    
    
    secondMomentum[(secondMomentum > 0)] = 1
    secondMomentum[(secondMomentum < 0)] = 0

    lastMomentum[(lastMomentum > 0)] = 1
    lastMomentum[(lastMomentum < 0)] = 0
    
    factorMomentum = firstMomentum + secondMomentum + lastMomentum 
    
    ptfWeightHisto =  factorMomentum.div(factorMomentum.sum(axis=1), axis = 0)
    ptfWeightHisto.fillna(0, inplace=True)
    ptfWeightHisto.iloc[lastMM:-1,:].to_csv("Testweight.csv")

# Stratégie consistant à séléctionner de manière equipondéré les 50 titres les plus performants sur les 3 derniers mois.
def priceMomentumS2(histoPriceV):
    # On itère dans notre histoPrice

    periode = 3
    nbStock = 20
    monthlyHistoPrice = histoPriceV.iloc[histoPriceV.reset_index().groupby(histoPriceV.index.to_period('M'))['index'].idxmax()]

    # On va créer un dataframe perf 1 mois futur et perf 1 mois passé
    momentumStrategy = (monthlyHistoPrice / monthlyHistoPrice.shift(periode) -1)[3:]
    momentumStrategy[(momentumStrategy.rank(axis = 1, method = "max", na_option = "top") > len(momentumStrategy.columns)-nbStock)] = 1
    momentumStrategy[(momentumStrategy.rank(axis = 1, method = "max", na_option = "top") <= len(momentumStrategy.columns)-nbStock)] = 0
    
    momentumStrategy = momentumStrategy.div(momentumStrategy.sum(axis = 1), axis = 0)
    return momentumStrategy.iloc[:-1,:]

# Stratégie consistant à séléctionner de manière equipondéré les 50 titres les plus performants sur les 3 derniers mois.
def priceMomentumLS(histoPriceV):
    # On itère dans notre histoPrice

    periode =  3
    nbStock = 20
    monthlyHistoPrice = histoPriceV.iloc[histoPriceV.reset_index().groupby(histoPriceV.index.to_period('M'))['index'].idxmax()]
    # On va créer un dataframe perf 1 mois futur et perf 1 mois passé
    histoReturn = (monthlyHistoPrice / monthlyHistoPrice.shift(periode) -1)[3:]
    momentumStrategyLong = histoReturn.copy()
    momentumStrategyShort = histoReturn.copy()
    momentumStrategyLong[(momentumStrategyLong.rank(axis = 1, method = "max", na_option = "top") > len(momentumStrategyLong.columns)-nbStock)] = 1 
    momentumStrategyLong[(momentumStrategyLong.rank(axis = 1, method = "max", na_option = "top") <= len(momentumStrategyLong.columns)-nbStock)] = 0
    
    momentumStrategyShort[(momentumStrategyShort.rank(axis = 1, method = "min", na_option = "bottom") <= nbStock) <0] = -1 
    momentumStrategyShort[(momentumStrategyShort.rank(axis = 1, method = "min", na_option = "bottom") > nbStock) > 0] = 0
    momentumStrategy =  momentumStrategyShort + momentumStrategyLong
    momentumStrategy = momentumStrategy.div(nbStock, axis = 0)
    return momentumStrategy.iloc[:-1,:]

def randomWeight(histoPriceV):  
    monthlyHistoPrice = histoPriceV.iloc[histoPriceV.reset_index().groupby(histoPriceV.index.to_period('M'))['index'].idxmax()]
    monthlyWeight = pd.DataFrame(np.random.randint(2,size=(len(monthlyHistoPrice.index),len(monthlyHistoPrice.columns))))
    monthlyWeight.index = monthlyHistoPrice.index
    monthlyWeight.columns = monthlyHistoPrice.columns
    monthlyWeightTemp = monthlyHistoPrice
    monthlyWeightTemp = monthlyWeightTemp / monthlyWeightTemp.shift(1)
    monthlyWeightTemp[monthlyWeightTemp > 0 ] = 1
    monthlyWeight = monthlyWeight * monthlyWeightTemp
    randomWeightFinal = monthlyWeight.div(monthlyWeight.sum(axis=1), axis = 0)  
    return randomWeightFinal.iloc[1:-1,:]


def lowVolStrategy(histoPrice):
    periodeDay = 30
    nbStock = 20
    histoReturn = histoPrice / histoPrice.shift(1) - 1
    histoRollingReturn = histoReturn.rolling(window = periodeDay, center = False).apply( lambda x: np.prod(1 + x) - 1)[periodeDay:]
    histoVol = (histoReturn.rolling(window = periodeDay, center = False).std() * math.sqrt(250) *100)[periodeDay:]
    monthlyHistoVol = histoVol.iloc[histoVol.reset_index().groupby(histoVol.index.to_period("M"))['index'].idxmax()] 
    monthlyHistoRollingReturn = histoRollingReturn.iloc[histoRollingReturn.reset_index().groupby(histoRollingReturn.index.to_period("M"))['index'].idxmax()] 
    monthlyHistoVolcc = monthlyHistoVol.copy()
#    monthlyHistoVol[(monthlyHistoVolcc.rank(axis = 1, method = "max", na_option = "top") > len(monthlyHistoVolcc.columns)-nbStock)] = 1
#    monthlyHistoVol[(monthlyHistoVolcc.rank(axis = 1, method = "max", na_option = "top") <= len(monthlyHistoVolcc.columns)-nbStock)] = 0

    monthlyHistoVol[(monthlyHistoVolcc.rank(axis = 1, method = "min", na_option = "bottom") <= nbStock)] = 1
    monthlyHistoVol[(monthlyHistoVolcc.rank(axis = 1, method = "min", na_option = "bottom") > nbStock)] = 0
    monthlyHistoVol[(monthlyHistoRollingReturn < 0)] = 0
    monthlyWeight = monthlyHistoVol.div(monthlyHistoVol.sum(axis = 1), axis = 0)
#    print (monthlyHistoVol)
    
    return monthlyWeight.iloc[:-1,:]



def forMyFriendBabil(histoPrice, histoWeight):
    
    histoReturn = histoPrice/ histoPrice.shift(1)
    histoReturn  = histoReturn.iloc[histoReturn.index.get_loc(histoWeight.index[0].date(), method = "backfill"):,:]
    histoReturn = histoReturn.reset_index()
#    histoPrice['ptfDate'] = histoWeight.index.get_loc(histoPrice['index'], method = 'pad')
    histoReturn['ptfDate'] = histoReturn.apply(lambda x: histoWeight.index.get_loc(x['index'], method = 'pad'), axis = 1)
    histoReturn = histoReturn.set_index("index")
    histoReturn = histoReturn.groupby(histoReturn['ptfDate']).apply( lambda x: np.prod(x.iloc[:,:-1]))
    print(histoReturn)
#    print (histoPrice)
#    print (histoPrice)
    
dateparse = lambda x: pd.datetime.strptime(x, '%d/%m/%Y')
#histoPrice = pd.read_csv("histoprice.csv",";", index_col = 0,parse_dates= True, date_parser=dateparse)

#
##priceMomentumAnalysis(histoPrice)
#histoPortfolio = pd.read_csv("Testweight.csv",",", index_col = 0,parse_dates= True)

histoPrice = pd.read_csv("Dow jones.csv",";", index_col = 0,parse_dates= True, date_parser=dateparse).iloc[:,:-1]
bench = pd.read_csv("Dow jones.csv",";", index_col = 0,parse_dates= True, date_parser=dateparse).iloc[:,-1]
perfBench = bench.iloc[-1] / bench.iloc[0] *100

histoPerf = pd.Series([perfBench])

strategy2 = priceMomentumS2(histoPrice)
strategyLS = priceMomentumLS(histoPrice)
strategyLowVol = lowVolStrategy(histoPrice)

forMyFriendBabil(histoPrice.copy(), strategyLS)

#
#histoValuePtf = calculPerf(strategy2, histoPrice)
##print (histoValuePtf)
#histoPerf = histoPerf.append(pd.Series([histoValuePtf.iloc[-1,0]]), ignore_index = True)
#
#histoValuePtf = calculPerf(strategyLS, histoPrice)
#histoPerf = histoPerf.append(pd.Series([histoValuePtf.iloc[-1,0]]), ignore_index = True)
#
#histoValuePtf = calculPerf(strategyLowVol, histoPrice)
#histoPerf = histoPerf.append(pd.Series([histoValuePtf.iloc[-1,0]]), ignore_index = True)
#
#rank1 = histoPrice.rank(axis = 1, method = "min", na_option = "top")
#rank2 = histoPrice.rank(axis = 1, method = "min", na_option = "bottom")
#test = rank1 - rank2
#
#lowVolStrategy(histoPrice)
#
#for i in range(0,5):
#    histoPortfolio = randomWeight(histoPrice)
#    histoValuePtf = calculPerf(histoPortfolio,histoPrice)
#    s = pd.Series([histoValuePtf.iloc[-1,0]])
#    histoPerf = histoPerf.append(s, ignore_index = True)
#    
#print("Perf du benchmark ", histoPerf.iloc[0])
#print("NAV de la stratégie momentum numéro 2 long only: ", histoPerf.iloc[1])
#print("NAV de la stratégie momentum numéro 2 long short: ", histoPerf.iloc[2])
#print("NAV de la stratégie Low Vol", histoPerf.iloc[3])
#
#print("NAV minimum des stratégies aléatoires:", histoPerf.iloc[4:].min())
#print("NAV maximum des stratégies aléatoires:", histoPerf.iloc[4:].max())
#print("NAV moyenne des stratégies aléatoires:", histoPerf.iloc[4:].mean())
#
#
