tablesOfColoumnName={} # map of { tableName: [coloumn names in metadata file] }
tablesOfValues={} # map of { tableName: [[coloumn values in csv file]] }
def readtxt ():
        file = open("metadata.txt","r")
        tableKaNaam = False
        table_Name = ""
        Line = file.readlines()
        for currLine in Line:    
            currLine = currLine.strip()
            if(currLine == "<begin_table>"):
                tableKaNaam = True
                table_Attributes = []
            elif(currLine == "<end_table>"):
                tablesOfColoumnName[table_Name] = table_Attributes     
            elif(tableKaNaam):
                table_Name = currLine
                tablesOfValues[table_Name] = {}
                tableKaNaam = False
            else:
                tablesOfValues[table_Name][currLine] = []
                table_Attributes.append(currLine)
        #print(tablesOfValues)        
        return tablesOfColoumnName 

def fillValues(database):
        for table in database.keys():
            f = open('./'+str(table)+'.csv')
            for line in f:
                values = [int(value) for value in line.strip().split(',')]
                columnKeNaam = tablesOfColoumnName[table]
                for i in range(0,len(columnKeNaam)):
                    tablesOfValues[table][columnKeNaam[i]].append(values[i])      


database = readtxt()
fillValues(database)
print(tablesOfValues)
