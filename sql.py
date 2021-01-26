import sys
import itertools
import sqlparse
import operator
tablesOfColoumnName={} # map of { tableName: [coloumn names in metadata file] }
databaseTable={} # map of { tableName: [[coloumn values in csv file]] }
colll = {}

def errorChecking(orderBypos, queryKaList, colll, grpbyPos, tablesPos, distinctMila, coloumnPos):

    # checking presence of from keyword
    if tablesPos == -10:
        print("Error: From keyword Absent in query")
        exit(0)
    
    index = ""
    #computing column name with order by
    if orderBypos != -10:
        if(len(queryKaList[orderBypos].split(" desc")) == 2):
           index = queryKaList[orderBypos].split(" desc")[0]
        else:
           index = queryKaList[orderBypos].split(" asc")[0]

        index = index.replace(" ","")

        # checking order by col is in metadata or not
        if index not in colll:
            print("Error: order by column "+index+" not in metadata")
            exit(0)

     # checking grp by col is in metadata or not
    if grpbyPos != -10:
        if queryKaList[grpbyPos] not in colll:
            print("Error: group by column "+queryKaList[grpbyPos]+" not in metadata")
            exit(0)

    # checking grp by col is same as order by col
    if grpbyPos != -10 and orderBypos != -10:
        if queryKaList[grpbyPos] != index:
            print("Error: Grp by column not same as Order by column")
            exit(0) 

    if ((tablesPos == 3 and distinctMila == False) or (tablesPos == 4 and distinctMila)):
        pass
    else:
        print("Error: Invalid Query as tableNames not at desired positions")
        exit(0)

    tables = queryKaList[tablesPos].replace(" ","").split(",")
    #print(tables)         

    metaTable = []
    for t in tablesOfColoumnName.keys():
        metaTable.append(t)
    
    #checking if tables in query in metadata or not
    for table in tables:
        if table not in metaTable:
            print("Error: tablename "+table+" not in metadata")
            exit(0)

    
    columns = queryKaList[coloumnPos].replace(" ","").split(",")

    #checking if col in select in metadata or not
    for col in columns:
        tc = ""
        if col != "*":
            if col.find(")") != -1:
                tc = col.split("(")[1].split(")")[0]
                if tc != "*":
                    if tc not in colll:
                        print("Error: Column " + tc +" used in select is not in metadata")
                        exit(0)
            else:
                if col not in colll:
                    print("Error: Column " + col+ " used in select is not in metadata")
                    exit(0)

    return tables,columns

def colPrint(colList):
    temp = []
    for col in colList:
        if col!="*" and col.find("(") == -1:
            for key in tablesOfColoumnName.keys():
                #print(key)
                #print(tablesOfColoumnName[keys])
                if col in tablesOfColoumnName[key]:
                    temp.append(key + "." + col.lower())
        else:
            temp.append(col.lower())
    
    print(temp)

def distinctHelper(finalResult):
    finalResult1 = []
    for tuples in finalResult:
        if tuples not in finalResult1:
            finalResult1.append(tuples)
        
    for tuples in finalResult1:
        print(tuples)
    print("\n\n\n")
    exit(0)

def orderBy(currResult, grpbyPos, joinKeColumnNames, selectcolumns, queryKaList, orderBypos):
    #print("orderBy ke andar\n\n\n")
    
    orderbyCol = queryKaList[orderBypos]

    if queryKaList[orderBypos].find("desc") != -1:
        orderbyCol = queryKaList[orderBypos].split(" desc")[0]

    elif queryKaList[orderBypos].find("asc") != -1:
        orderbyCol = queryKaList[orderBypos].split(" asc")[0]
    
    if grpbyPos != -10:
        grpByCol = queryKaList[grpbyPos] 
    
    #print(orderbyCol,"bhai")
    count = 0
    count1 = 0
    #print(joinKeColumnNames,"bhai")
    if grpbyPos != -10:
        for col in selectcolumns:
            if col == grpByCol:
                break
            count = count + 1
    else:
        for col in joinKeColumnNames:
            if col == orderbyCol:
                break
            count1 = count1 + 1

    #print(count, count1)
    #print(grpbyPos)
    if grpbyPos != -10:
        if(queryKaList[orderBypos].find("desc") != -1):
            currResult = sorted(currResult, key=lambda x: x[count],reverse = True)

        else:
            currResult = sorted(currResult, key=lambda x: x[count])
    else:
        if(queryKaList[orderBypos].find("desc") != -1):
            currResult = sorted(currResult, key=lambda x: x[count1],reverse = True)

        else:
            currResult = sorted(currResult, key=lambda x: x[count1])
    return currResult

def extractAgg(columns):
    #print(columns)
    aggrPairList = []
    aggCount = 0
    for col in columns:
        if col[len(col)-1] == ")":
            aggrPairList.append( [col.split("(")[0], col.split("(")[1].split(")")[0]] )
            aggCount = aggCount + 1
        else:
            aggrPairList.append(["chirag", col])
    return aggrPairList, aggCount

def aggregateHelper(grpbyMap, aggrPairList, isList, joinKeColumnNames, queryKaList, grpbyPos):
    #print(grpbyMap)
    #print("aggregateHelper ke andar\n\n\n")
    aggResult = []
    
    #print(isList)
    if(isList):
        listT = []
        listT = list(map(list, zip(*grpbyMap)))
        i=0
        while i < len(aggrPairList):
            #print(aggrPairList[i][0], aggrPairList[i][1])
            funct = aggrPairList[i][0]
            col = aggrPairList[i][1]
            if(col != "*"):
                try:
                    pos = joinKeColumnNames.index(col)
                except:
                    print("Error: Column Name " + col +" is not in database")
                    exit(0)

            if funct == "min" or funct == "MIN":
                aggResult.append(min(listT[pos]))
            elif funct == "max" or funct == "MAX":
                aggResult.append(max(listT[pos]))
            elif funct == "avg" or funct == "AVG":
                aggResult.append(sum(listT[pos])/len(listT[pos]))
            elif funct == "sum" or funct == "SUM":
                aggResult.append(sum(listT[pos]))
            elif funct == "count" or funct == "COUNT":
                if col == "*":
                    aggResult.append(len(listT[0]))
                else:
                    aggResult.append(len(listT[pos]))
            i = i + 1
        return aggResult
    else:
        #print("grp")
        for key in grpbyMap:
            list1 = [] 
            list1 = grpbyMap[key]
            list2 = []
            if len(list1) == 1:
                list1 = list(list1)
            list2 = list(map(list, zip(*list1)))
            #print(list2)
            temp = []
            #print(key)
            #temp.append(key)
            i=0
            #print(aggrPairList,"lvde")
            while i < len(aggrPairList):
                #print(aggrPairList[i][0], aggrPairList[i][1])
                funct = aggrPairList[i][0]
                col = aggrPairList[i][1]
                if col == queryKaList[grpbyPos] and funct == "chirag":
                    temp.append(key)

                pos = 0
                if(col != "*"):
                    pos = joinKeColumnNames.index(col)
                
                #print(col, funct, pos)
                #print(list2[pos])
                if funct == "min" or funct == "MIN":
                    temp.append(min(list2[pos]))
                elif funct == "max" or funct == "MAX":
                    temp.append(max(list2[pos]))
                elif funct == "avg" or funct == "AVG":
                    temp.append(sum(list2[pos])/len(list2[pos]))
                elif funct == "sum" or funct == "SUM":
                    temp.append(sum(list2[pos]))
                elif funct == "count" or funct == "COUNT":
                    temp.append(len(list1))
                i = i + 1
            aggResult.append(temp)

    #print("final\n") 
    #print(aggResult)
    return aggResult

def groupByhelp(whereKaResult, queryKaList, columns, joinKeColumnNames, groupByCol):
    #print("Group By helper\n\n\n")
    #print(groupByCol)
    colSize = len(columns)
    #print(colSize)
    columnNameofGrpBy = queryKaList[groupByCol]
    #print(columnNameofGrpBy)
    colMila = False
    #print(columns)
    #print(columnNameofGrpBy)
    #aggrPairList = []
    for col in columns:
        #print(col)
        if col[len(col)-1] == ")":
            colSize = colSize - 1

        if col == columnNameofGrpBy:
            colMila = True
            colSize = colSize - 1

    if colMila == False:
        print("Error: Group by column not in select clause")
        exit(0)
    
    if colSize != 0:
        print("Error: Extra coloumns in select which are not in Group by. If you want it then add it with aggr function")
        exit(0)

    pos =  joinKeColumnNames.index(columnNameofGrpBy)
    #print(pos)

    grpbyMap = {}

    for tuple in whereKaResult:
        grpbyMap[tuple[pos]] = []
    
    for tuple in whereKaResult:
        grpbyMap[tuple[pos]].append(tuple)
    
    #print(grpbyMap)
    return grpbyMap, pos

def whereHelper(queryList, wherePos, joinKeColumnNames, joinvalues, columns):
    #print("whereHelper ke andar\n\n\n")
    condition=str(queryList[wherePos].replace(" ",""))
    condition = condition[5:]
    isAnd=False
    isOr=False
    #print(condition)
    left_condition=condition
    right_condition=""
        
    if(len(condition.split("and"))==2):
        left_condition=condition.split("and")[0]
        right_condition=condition.split("and")[1]
        if len(right_condition) == 0 or len(left_condition) == 0:
            print("Error: Incorrect query")
            exit(0)
        isAnd = True
            
    elif(len(condition.split("or"))==2):
        left_condition=condition.split("or")[0]
        right_condition=condition.split("or")[1]
        if len(right_condition) == 0 or len(left_condition) == 0:
            print("Error: Incorrect query")
            exit(0)
        isOr = False    

    left_condition = left_condition.replace(" ","")
    right_condition = right_condition.replace(" ","")

    #print(left_condition, right_condition)
    operator1 = ""
    operator2 = ""

    if left_condition.find("==")!= -1:
        print("Error: == not used in SQL query. Use = instead")
        exit(0)
    
    if len(right_condition) > 1 and right_condition.find("==")!=-1:
        print("Error: == not used in SQL query. Use = instead")
        exit(0)

    for op in ["<=", ">=", "<", ">", "="]:
        if(left_condition.find(op) != -1):
            operator1 = op
            break
    
    if(len(right_condition) > 1):
        for op in ["<=", ">=", "<", ">", "="]:
            if(right_condition.find(op) != -1):
                operator2 = op
                break

    col1 = left_condition.split(operator1)[0]
    col2 = left_condition.split(operator1)[1]
    #print(col1, col2) 
    #print(operator1, operator2)
    rightCondPresent = False
    if(len(right_condition) > 1):
        rightCondPresent = True
        col3 = right_condition.split(operator2)[0]
        col4 = right_condition.split(operator2)[1]
        #print(col3,col4)

    coloumnFound = [False, False, False, False]
    for col in joinKeColumnNames:
        if(col == col1):
            coloumnFound[0] = True
        elif(col == col2):
            coloumnFound[1] = True
        elif(rightCondPresent and  col == col3):
            coloumnFound[2] = True
        elif(rightCondPresent and col == col4):
            coloumnFound[3] = True

    
    if(coloumnFound[0]==False):
        print("Column "+col1+" is not in database")
        exit(0)
    elif(coloumnFound[1]==False and (col2.isnumeric() == False and col2[0] != "-")):
        print("Column "+col2+" is not in database")
        exit(0)
    elif(rightCondPresent and coloumnFound[2]==False):
        print("Column "+col3+" is not in database")
        exit(0)
    elif(rightCondPresent and (col4.isnumeric() == False and col4[0] != "-") and coloumnFound[3]==False):
        print("Column "+col4+" is not in database")
        exit(0)

    columnIndex = [-1, -1, -1, -1]

    for i in range(0, len(joinKeColumnNames)):
        if(joinKeColumnNames[i] == col1):
            columnIndex[0] = i
        elif(joinKeColumnNames[i] == col2):
            columnIndex[1] = i
        elif(rightCondPresent and joinKeColumnNames[i] == col3):
            columnIndex[2] = i      
        elif(rightCondPresent and joinKeColumnNames[i] == col4):
            columnIndex[3] = i

    #print(columnIndex)

    iscol2Num = False
    iscol4Num = False

    if columnIndex[1] == -1:
        iscol2Num = True
    
    if columnIndex[3] == -1:
        iscol4Num = True
    
    finalAns = []
    dictOperator = { '=': operator.eq, '<=': operator.le, '>=': operator.ge , '<': operator.lt, '>': operator.gt}
    for i in joinvalues:
        #print(i)
        #print(isOr, isAnd)
        if isOr and rightCondPresent:
            if iscol2Num==False and iscol4Num==False and dictOperator[operator1](int(i[columnIndex[0]]), int(i[columnIndex[1]])) or dictOperator[operator2](int(i[columnIndex[2]]), int(i[columnIndex[3]])):
                finalAns.extend([i])
            elif iscol2Num==True and iscol4Num==False and dictOperator[operator1](int(i[columnIndex[0]]), int(col2)) or dictOperator[operator2](int(i[columnIndex[2]]), int(i[columnIndex[3]])):
                finalAns.extend([i])
            elif iscol2Num==False and iscol4Num==True and dictOperator[operator1](int(i[columnIndex[0]]), int(i[columnIndex[1]])) or dictOperator[operator2](int(i[columnIndex[2]]), int(col4)):
                finalAns.extend([i])
            elif iscol2Num==True and iscol4Num==True and dictOperator[operator1](int(i[columnIndex[0]]), int(col2)) or dictOperator[operator2](int(i[columnIndex[2]]), int(col4)):
                finalAns.extend([i])

        elif isAnd and rightCondPresent:
            if iscol2Num==False and iscol4Num==False and dictOperator[operator1](int(i[columnIndex[0]]), int(i[columnIndex[1]])) and dictOperator[operator2](int(i[columnIndex[2]]), int(i[columnIndex[3]])):
                finalAns.extend([i])
            elif iscol2Num==True and iscol4Num==False and dictOperator[operator1](int(i[columnIndex[0]]), int(col2)) and dictOperator[operator2](int(i[columnIndex[2]]), int(i[columnIndex[3]])):
                finalAns.extend([i])
            elif iscol2Num==False and iscol4Num==True and dictOperator[operator1](int(i[columnIndex[0]]), int(i[columnIndex[1]])) and dictOperator[operator2](int(i[columnIndex[2]]), int(col4)):
                finalAns.extend([i])
            elif iscol2Num==True and iscol4Num==True and dictOperator[operator1](int(i[columnIndex[0]]), int(col2)) and dictOperator[operator2](int(i[columnIndex[2]]), int(col4)):
                finalAns.extend([i])
        else:
            if iscol2Num==True and dictOperator[operator1](int(i[columnIndex[0]]), int(col2)):
                #print(i)
                finalAns.extend([i])
            elif iscol2Num==False and dictOperator[operator1](int(i[columnIndex[0]]), int(i[columnIndex[1]])):
                #print(i)
                finalAns.extend([i])
    
    #print(finalAns)
    return finalAns
   
def readtxt ():
        file = open("metadata.txt","r")
        tableKaNaam = False
        table_Name = ""
        Line = file.readlines()
        colll = []
        for currLine in Line:    
            currLine = currLine.strip()
            if(currLine == "<begin_table>"):
                tableKaNaam = True
                table_Attributes = []
            elif(currLine == "<end_table>"):
                tablesOfColoumnName[table_Name] = table_Attributes     
            elif(tableKaNaam):
                table_Name = currLine
                databaseTable[table_Name] = {}
                tableKaNaam = False
            else:
                databaseTable[table_Name][currLine] = []
                table_Attributes.append(currLine)
                colll.append(currLine)
        #print(databaseTable)        
        return tablesOfColoumnName,colll 

def fillValues(database):
        for table in database.keys():
            f = open('./'+str(table)+'.csv')
            for line in f:
                line = line.strip("\n")
                values = []
                for value in line.split(','):
                    if value != '':
                        value = value.strip("\'\"")
                        values.append(int(value))
                columnKeNaam = tablesOfColoumnName[table]
                for i in range(0,len(columnKeNaam)):
                    databaseTable[table][columnKeNaam[i]].append(values[i])      

def querySeperate(query, database, colll):
    if(query[-1:] != ";"):
        print("Error: ; missing in SQL query")
        exit(0)

    query = query[:-1]
    query = sqlparse.format(query, keyword_case='lower')
    #print(query)
    parsed = sqlparse.parse(query)
    statement = parsed[0]

    tokenlist = sqlparse.sql.IdentifierList(statement.tokens).get_identifiers()

    queryKaList = []

    for i in tokenlist:
        queryKaList.append(str(i))

    #print(queryKaList)

    if(len(queryKaList) < 4):
        print("Error: Invalid SQL query")
        exit(0)

    if len(queryKaList[0]) != 6:
        print("Error: select keyword missing")
        exit(0) 
    
    tablesPos = -10
    wherePos = -10
    grpbyPos = -10
    orderBypos = -10
    distinctMila = False
    coloumnPos = 1

    queryOrder = []

    for i in range(0, len(queryKaList)):
        if(queryKaList[i] == "distinct"):
            if (tablesPos == -10 and wherePos == -10 and grpbyPos == -10 and orderBypos == -10):
                distinctMila = True
                coloumnPos = 2
            else:
                print("Error: Incorrect Query")
                exit(0)

        if(queryKaList[i] == "from"):
            if tablesPos == -10 and wherePos == -10 and grpbyPos == -10 and orderBypos == -10:
                tablesPos = i+1
            else:
                print("Error: Incorrect Query")
                exit(0)

        if queryKaList[i].find("where") != -1:
            if wherePos == -10 and grpbyPos == -10 and orderBypos == -10:
                wherePos = i
            else:
                print("Error: Incorrect Query")
                exit(0)
              
        if queryKaList[i] == "group by":
            if grpbyPos == -10 and orderBypos == -10:
                grpbyPos = i+1
            else:
                print("Error: Incorrect Query")
                exit(0)
            
        if queryKaList[i] == "order by":
            if orderBypos == -10:
                orderBypos = i+1
            else:
                print("Error: Incorrect Query")
                exit(0)

    tables, columns = errorChecking(orderBypos, queryKaList, colll, grpbyPos, tablesPos, distinctMila, coloumnPos)

    if(wherePos != -10):
        whereCond = queryKaList[wherePos]
        whereCond = whereCond.replace(" ","")
    
    tq = []
    joinKeColumnNames = []
    joinvalues = [] 
    for values in tables:
        pq = []
        for keyvalues in databaseTable[values]:
            joinKeColumnNames.append(keyvalues)
            pq.append(databaseTable[values][keyvalues])
        cq = []
        cq = list(map(list, zip(*pq)))
        tq.append(cq)

    for elements in itertools.product(*tq):
        joinvalues.append(sum(list((elements)),[]))       

    aggrPairList = []
    aggrPairList,aggCount = extractAgg(columns)


    if(wherePos != -10):
        joinvalues = whereHelper(queryKaList, wherePos, joinKeColumnNames, joinvalues, columns)
    
    grpbyMap = {}
    pos = 0
    if(grpbyPos != -10):
        grpbyMap, pos = groupByhelp(joinvalues, queryKaList, columns, joinKeColumnNames, grpbyPos)

    if(grpbyPos == -10 and aggCount > 0):
        joinvalues = aggregateHelper(joinvalues, aggrPairList,1, joinKeColumnNames, queryKaList, grpbyPos)
        #print(joinvalues)

    #group by is present in query
    elif aggCount > 0 and grpbyPos != -10:
        joinvalues = aggregateHelper(grpbyMap, aggrPairList, 0, joinKeColumnNames, queryKaList, grpbyPos)
    
    elif grpbyPos != -10:
        temp = []
        for key in grpbyMap.keys():
            temp.append([key])

        #print(temp)
        
        joinvalues = temp
        if orderBypos != -10 and aggCount != len(columns):
            joinvalues = orderBy(joinvalues, grpbyPos,joinKeColumnNames, columns, queryKaList, orderBypos)
            colPrint(columns)
            #print(columns)
            if distinctMila:
                distinctHelper(joinvalues)
            else:
                for tuples in joinvalues:
                    print(tuples)
                print("\n\n\n")
                exit(0) 
        else:
            colPrint(columns)
            if distinctMila:
                distinctHelper(joinvalues)
            else:
                for tuples in joinvalues:
                    print(tuples)
                print("\n\n\n")
                exit(0)

    #print(aggResult)
    if orderBypos != -10 and aggCount != len(columns):
        joinvalues = orderBy(joinvalues, grpbyPos,joinKeColumnNames, columns, queryKaList, orderBypos)
    
    if aggCount == len(columns):
        #print("inside agg printing")
        #print aggregate col as schema and joinke columns as op
        colPrint(columns)
        print(joinvalues)
        print("\n\n\n")
        exit(0)

    if grpbyPos != -10 and aggCount > 0:
        #print("inside grp by print\n")
        colPrint(columns)
        for tuples in joinvalues:
            print(tuples)
        
        print("\n\n\n")
        exit(0)

    #if * is used in column name
    if columns[0] == "*":
        colPrint(joinKeColumnNames)
        #print(joinKeColumnNames)
        if distinctMila:
            distinctHelper(joinvalues)
        else:
            for tuples in joinvalues:
                print(tuples)
            print("\n\n\n")
            exit(0)


    finalResult = []
    for tuples in joinvalues:
        temp = []
        for col in columns:
            pos = joinKeColumnNames.index(col)
            temp.append(tuples[pos])
        finalResult.append(temp)

    colPrint(columns)

    if distinctMila:
        distinctHelper(finalResult)
    else:
        for tuples in finalResult:
            print(tuples)
    
    print("\n\n\n")

def main():
    if len(sys.argv) != 2:    
        print("Wrong input format")    # python3 fileName.py "quemry"
        exit(0)
    query = sys.argv[1]
    database, colll = readtxt()
    fillValues(database)
    querySeperate(query, database, colll)

main()
