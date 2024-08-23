##########################################################################################################
#
# NOTE:
# impegare come nel seguente esempio:
#
# import df_s3_other_functions
# UOF = df_s3_other_functions.functions(dframe=self._df, imps={'ps_functions': PSF, 'ps_types': None, 'regex': None})
# 
# UOF.fixCol('cognome')
#
##########################################################################################################

class functions:

    def __init__(self,dframe, imps = {'ps_functions':None,'ps_types':None,'regex':None}):
        
        if imps['ps_functions'] is None: 
            import pyspark.sql.functions as imps_ps_functions
            imps['ps_functions'] = imps_ps_functions
        if imps['ps_types'] is None: 
            import pyspark.sql.types as imps_ps_types
            imps['ps_types'] = imps_ps_types
        if imps['regex'] is None: 
            import re as imps_regex
            imps['regex'] = imps_regex
        
        self.dframe = dframe
        self.imps = imps


    #ottieni datatype di un attributo
    def dType(self,colname):  

        coltype = None
        for field in self.dframe.schema.fields:
            if field.name == colname:
                coltype = field.dataType
                break
                
        return coltype
        
    ##########################################################################################################
          
    #ARGOMENTO   | DEFAULT                       | DATATYPE ATTRB.  | DATATYPE APPLCZ. |DESCRIZIONE
    #-----------------------------------------------------------------------------------------------------------------
    #colname     |                               |str               | *                | nome colonna nel dataframe
    #collength   | None                          |int               | *                | limite substring e padding
    #padval      | None                          |str               | *                | valore del padding
    #padpos      | 'r'                           |str               | *                | posizione del padding ('r'/'l')
    #placeholder | None                          |str               | *                | valore di coalescing, applicato 
    #colformat   | None/yyyy-MM-dd/['S','N']     |str/str/arr[str]  | num/date/bool    | formattazione numero/data/booleano 
    #coltrim     | False                         |bool              | str              | trim della stringa originale
    #stringcast  | False                         |bool              | *                | cast preventivo dell'attributo a stringa 
    #remempty    | False                         |bool              | str              | i valori '' sono convertiti a null
    #typeraise   | True                          |bool              | *                | solleva errore se datatype non supportato
    #skipcheck   | False                         |bool              | *                | salta verifiche su argomenti 
        
    ##########################################################################################################
     
    def fixCol(self,colname,collength = None,padval = None,padpos = 'r',placeholder = None,colformat = None,coltrim = False,stringcast = False,remempty = False,typeraise = True,skipcheck = False):
          
        colres = self.imps['ps_functions'].col(colname)
        coltype = self.dType(colname)
        
        #applica padding e substring (solo tipo str)
        def addpad(colres,collength,padval,padpos,do_substr = True):
        
            #verifiche
            if not skipcheck:
                #verifica padpos
                if padpos is not None and padpos not in ['l','r']:
                    raise ValueError('Valore di padding non supportato \n valori support.: \'r\',\'l\'')
                #verifica collength
                if collength is not None and (not isinstance(collength,int) or collength < 0):
                    raise ValueError('Valore di lunghezza stringa non supportato \n valori support.: int > 0')
        
            if collength is not None and collength > 0:
            
                #coalesce con vuoto
                colres = self.imps['ps_functions'].coalesce(colres,self.imps['ps_functions'].lit(''))
                
                #applica substring
                if do_substr: 
                    colres = self.imps['ps_functions'].substring(colres,1,collength)
                    
                #applica padding
                if padval is not None: 
                    if padpos == 'r':
                        colres = self.imps['ps_functions'].rpad(colres,collength,padval)
                    elif padpos == 'l':
                        colres = self.imps['ps_functions'].lpad(colres,collength,padval)
            return colres
            
          
        #applica placeholder di tipo stringa (solo tipo str)
        def addphs(colres,placeholder,dftph = True):
        
            #verifiche
            if not skipcheck:
            
                #verifica placeholder
                if placeholder is not None:
                    if not isinstance(placeholder,str):
                        raise TypeError(f"placeholder \"{placeholder}\" non Ã¨ di tipo stringa \n placeholder sugg.: \'\'")
        
            #placeholder di default se non fornito
            if placeholder is None and dftph == True:
                placeholder = ''
                       
            #applica placeholder
            if placeholder is not None:
                colres = self.imps['ps_functions'].when((colres.isNull()),self.imps['ps_functions'].lit(placeholder)).otherwise(colres)
      
            
            return colres
         
         
        #stringa o castato a stringa
        if isinstance(coltype, self.imps['ps_types'].StringType) or stringcast:
        
            if stringcast and not isinstance(coltype, self.imps['ps_types'].StringType): colres = colres.cast('string')
            if coltrim: colres = self.imps['ps_functions'].when(colres.isNotNull(),self.imps['ps_functions'].trim(colres)).otherwise(colres) 
            if remempty: colres = self.imps['ps_functions'].when(self.imps['ps_functions'].length(colres) == 0,self.imps['ps_functions'].lit(None)).otherwise(colres)
             
            #applica placeholder tipo stringa o valore vuoto
            colres = addphs(colres,placeholder)
            
            #applica padding e substr
            colres = addpad(colres,collength,padval,padpos)
         
         
        #timestamp/data
        elif isinstance(coltype, (self.imps['ps_types'].TimestampType,self.imps['ps_types'].DateType)):
            
            #valore formattazione custom
            colformat = colformat if colformat is not None else 'yyyy-MM-dd'
            
            #formatta data
            colres = self.imps['ps_functions'].when(colres.isNotNull(),self.imps['ps_functions'].date_format(colres,colformat))
            
            #cast a stringa
            colres = colres.cast('string')
            
            ##applica placeholder tipo stringa o valore vuoto
            colres = addphs(colres,placeholder)
            
            #applica padding e substr
            colres = addpad(colres,collength,padval,padpos)

                   
        #booleano
        elif isinstance(coltype, self.imps['ps_types'].BooleanType):
        
            #verifiche
            if not skipcheck:
            
                #verifica colformat
                if colformat is not None and (not isinstance(colformat, list) or not len(colformat) == 2):
                    raise ValueError("formatt. non valida. \n formatt. sugg.: ['S','N']")
        
            #valori formattazione default
            colformat = colformat if colformat is not None else ['S','N']
            
            #conversione
            colres = self.imps['ps_functions'].when(colres.isNull(),self.imps['ps_functions'].lit(placeholder)).when(colres == True,self.imps['ps_functions'].lit(colformat[0])).otherwise(self.imps['ps_functions'].lit(colformat[1]))
            
            #cast a stringa
            colres = colres.cast('string')
            
            #applica placeholder tipo stringa o valore vuoto
            colres = addphs(colres,placeholder)
            
            #applica padding
            colres = addpad(colres,collength,padval,padpos,False)
            
                                    
        #numerico
        elif isinstance(coltype, (self.imps['ps_types'].IntegerType,self.imps['ps_types'].LongType,self.imps['ps_types'].DoubleType,self.imps['ps_types'].DecimalType)):
        
            #TODO: ARROTONDAMENTO/TRONCAMENTO
            #NOTA: i decimal vengono convertiti a double, dunque troncati oltre una certa cifra

            #cast decimal a double
            if isinstance(coltype, self.imps['ps_types'].DecimalType):
                colres = colres.cast('double')
                
            #verifiche varie
            if not skipcheck:
            
                #verifica placeholder
                if placeholder is not None:
                    if (isinstance(coltype, (self.imps['ps_types'].IntegerType,self.imps['ps_types'].LongType)) and not isinstance(placeholder,(int,str))):
                        raise TypeError(f"placeholder \"{placeholder}\" non Ã¨ di tipo {coltype} o stringa \n placeholder sugg.: 0")
                    if (isinstance(coltype, (self.imps['ps_types'].DoubleType, self.imps['ps_types'].DecimalType)) and not isinstance(placeholder,(float,str))): 
                        raise TypeError(f"placeholder \"{placeholder}\" non Ã¨ di tipo {coltype} o stringa \n placeholder sugg.: 0.0")
            
                #verifica stringa formattazione
                if colformat is not None:
                       
                    #verifica caratteri generici
                    if '%' not in colformat:
                        raise ValueError("colformat non comprende \"%\", per estrazione a stringa")
                        
                    #verifica interi
                    if isinstance(coltype, (self.imps['ps_types'].IntegerType,self.imps['ps_types'].LongType)): 
                    
                         #info db
                        maxlen = self.dframe.select(self.imps['ps_functions'].max(self.imps['ps_functions'].length(self.imps['ps_functions'].coalesce(self.imps['ps_functions'].col(colname).cast('string'),self.imps['ps_functions'].lit(''))))).head()[0] #lunghezza massima intero
                        frmsugg = ('%0' + str(maxlen) + 'd')
                        
                        #verifica caratteri
                        if ('.' in colformat or 'f' in colformat or 'd' not in colformat):
                            raise ValueError(f"colformat \"{colformat}\" non valido per tipo {coltype} \n formatt. sugg.: {frmsugg}")
                        
                        #verifica caratteri (cifre + separatore)                
                        maxlen1 = self.dframe.select(self.imps['ps_functions'].max(self.imps['ps_functions'].length(self.imps['ps_functions'].col(colname)))).head()[0] #lunghezza massima intero in df
                        frmlen1 = self.imps['regex'].search(r'%0?(\d+)d', colformat)
                        if frmlen1: frmlen1 = int(frmlen1.group(1))
                        else: raise ValueError(f"colformat \"{colformat}\" non valido per tipo {coltype}")
                        if frmlen1 < maxlen1 : raise ValueError(f"carat. colformat \"{colformat}\" non sufficienti \n carat. max df: {maxlen1} \n carat. formatt: {frmlen1} \n formatt. min. sugg.: {frmsugg}")
                            
                    #verifica float
                    if isinstance(coltype, (self.imps['ps_types'].DoubleType,self.imps['ps_types'].DecimalType)):
                    
                        #info db
                        maxlen = self.dframe.select(self.imps['ps_functions'].max(self.imps['ps_functions'].length(self.imps['ps_functions'].regexp_replace(self.imps['ps_functions'].format_string('%f',self.imps['ps_functions'].col(colname).cast('double')),r'\.?0+$|(^null$)', '')))).head()[0] #lunghezza massima p.intera+separatore+decimale in df
                        maxlen2 = self.dframe.select(self.imps['ps_functions'].max(self.imps['ps_functions'].length(self.imps['ps_functions'].coalesce(self.imps['ps_functions'].split(self.imps['ps_functions'].regexp_replace(self.imps['ps_functions'].format_string('%f',self.imps['ps_functions'].col(colname).cast('double')),r'\.?0+$', ''), "\\.").getItem(1),self.imps['ps_functions'].lit(''))))).head()[0] #lunghezza massima parte decimale in df
                        maxlen1 = maxlen - maxlen2 - 1 #lunghezza massima parte intera in df
                        frmsugg = ('%0' + str(maxlen) + '.' + str(maxlen2) + 'f')
                    
                        #verifica caratteri
                        if ('d' in colformat or 'f' not in colformat or '.' not in colformat):
                            raise ValueError(f"formatt. \"{colformat}\" non valida per tipo {coltype} \n formatt. sugg.: {frmsugg}")
                        
                        #verifica formattazione                
                        frmlen = self.imps['regex'].search(r'%0?(\d+)\.(\d+)f', colformat)
                        if frmlen: 
                            frmlen2 = int(frmlen.group(2)) #lunghezza parte decimale in formattazione
                            frmlen = int(frmlen.group(1)) #lunghezza massima p.intera+separatore+decimale in formattazione
                            frmlen1 = frmlen - frmlen2 - 1 #lunghezza parte intera in formattazione
                        else: raise ValueError(f"formatt. \"{colformat}\" non valida per tipo {coltype} \n formatt. sugg.: {frmsugg}") 
                        if frmlen < maxlen : raise ValueError(f"carat. totali colformat \"{colformat}\" non sufficienti \n carat. max df: {maxlen} \n carat. formatt: {frmlen} \n formatt. min. sugg.: {frmsugg}")
                        if frmlen1 < maxlen1 : raise ValueError(f"carat. parte intera formatt. \"{colformat}\" non sufficienti \n carat. max df: {maxlen1} \n carat. formatt: {frmlen1} \n formatt. sugg.: {frmsugg}")
                        #if frmlen2 != maxlen2 : raise ValueError(f"carat. parte decimale formatt. \"{colformat}\" maggiore delle massime presenti \n carat. max df: {maxlen2} \n carat. formatt: {frmlen2} \n formatt. sugg.: {frmsugg}")
                                     
            #applica placeholder tipo numerico
            if placeholder is not None and isinstance(placeholder,(int,float)):
                colres = self.imps['ps_functions'].coalesce(colres,self.imps['ps_functions'].lit(placeholder))
                    
            #formatta numero a stringa
            if colformat is not None:
                colres = self.imps['ps_functions'].when(colres.isNotNull(),self.imps['ps_functions'].format_string(colformat,colres)).otherwise(colres.cast('string'))
            
            #cast a stringa
            colres = colres.cast('string')
                    
            #applica placeholder tipo stringa o valore vuoto
            if placeholder is None or (placeholder is not None and isinstance(placeholder,str)):
                colres = addphs(colres,placeholder)
                           
            #applica padding (no substr)
            colres = addpad(colres,collength,padval,padpos,False)
        
      
        #datatype non supportato
        elif typeraise: 
            raise TypeError(f"tipo non supportato: {coltype}")
                               
        return colres



    ##########################################################################################################
