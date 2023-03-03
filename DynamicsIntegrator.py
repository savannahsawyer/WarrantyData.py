"""Database logic
Handles getting data from DYNAMICSSLAPP view
"""

import pyodbc
from datetime import datetime
from csv import reader as csvReader

##################################
# Database settings
##################################

sql = {
    'connection': 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=MCKATLSQL;PORT=1433;DATABASE=MWA;UID=EnergyUser;PWD=tW3=yWqV%&HArg97',
    'query': """
        SELECT * FROM [MWA].[Queries].[WarrantyProjectTransactions]
    """
}


class dynamicsProjectsCost:
    def __init__(self):
        self.projectList = []
        self.projectLookup = {}
        self.numberCostsPulled = 0
        self.pullInProjects()

    def pullInProjects(self):
        # make a query to the Dynamics database, and pull down the results of the query
        print("Pulling the most recent warranty costs from Dynamics...")
        sql_connection = pyodbc.connect(sql.get('connection'))  # Connection to SQL Server
        cursor = sql_connection.cursor()  # get Cursor object using the connection
        cursor.execute(sql.get("query"))  # run query
        for i, row in enumerate(cursor):
            self.pullInTransaction(row)

        print("Completed pulling the recent warranty costs from Dynamics, with {0} results found".format(self.numberCostsPulled))

    def pullInTransaction(self, costRow):
        newCostRow = dynamicsCost(costRow)
        if newCostRow.projectId not in self.projectLookup:
            self.projectLookup[newCostRow.projectId] = []
        self.projectLookup[newCostRow.projectId].append(newCostRow)
        self.numberCostsPulled += 1

    def getProjectCosts(self, projectId):
        if projectId not in self.projectLookup:
            return []
        return self.projectLookup[projectId]

    def getCondensedJobCosts(self, projectId):
        allCosts = self.getProjectCosts(projectId)
        projectManagerCost = 0
        if len(allCosts) == 0:
            return [], projectManagerCost

        # this is where we are pulling all non project manager labor entries
        condensedData = dynamicsCondensedData()
        laborType = "LABR"
        for currRow in allCosts:
            assert isinstance(currRow, dynamicsCost), "The cost is not correct type ('{0}' given)".format(type(currRow))
            if currRow.accountingType == laborType and not currRow.isPM:
                # create a placeholder for all of the labor that isn't project manager associated (to isolate task codes)
                condensedData.addCostToGroup(currRow)

        for costTransaction in allCosts:
            assert isinstance(costTransaction, dynamicsCost), "The cost is not of correct type ('{0}' given)".format(
                type(costTransaction))
            # find all of warranty manager time and clump to spread across all calls per project
            if costTransaction.isPM:
                projectManagerCost += costTransaction.cost
            elif len(costTransaction.sourceID) > 0:
                condensedData.addCostToGroup(costTransaction)
            else:
                print("Not sure what to do with {0}...".format(costTransaction))
                print(costTransaction.accountingType, costTransaction.vendorCostDesc)

        return list(condensedData.groupLookup.values()), projectManagerCost



class dynamicsCost:
    def __init__(self, sqlRow):
        self.dataRow = sqlRow
        self.projectId = ""
        self.taskId = ""
        self.taskDesc = ""
        self.sourceID = "" # can be either a vendor ID or an employee ID
        self.sourceName = "" # can be either a vendor name or an employee name
        self.isVendor = True
        self.date = None
        self.units = 0
        self.cost = 0
        self.costType = ""
        self.accountingType = "" # from AccountID
        self.vendorCostDesc = ""
        self.poNumber = "" # po number - will only be applied when is a vendor
        self.invNumber = "" # invoice number - will only be applied when is a vendor
        self.projectManagerId = ""
        self.seniorPMId = ""
        self.isPM = False # tells whether or not the cost comes from a PM
        self.pullInFromSQLRow(sqlRow)

    def pullInFromCSVRow(self, csvRow):
        offsetCol = 0
        self.projectId = ""
        self.taskId = csvRow[offsetCol + 0][0:8].replace("-", "")
        self.costType = csvRow[offsetCol + 1]
        self.accountingType = csvRow[offsetCol + 8]
        self.vendorCostDesc = csvRow[offsetCol + 12]
        if len(csvRow[offsetCol + 2]) > 0:
            self.sourceID = csvRow[offsetCol + 2] # Employee id
            self.sourceName = csvRow[offsetCol + 5]  # Employee name
            self.isVendor = False
        elif len(csvRow[offsetCol + 3]) > 0:
            self.sourceID = csvRow[offsetCol + 3]  # Vendor id
            self.sourceName = csvRow[offsetCol + 4]  # Vendor name
            self.isVendor = True
        else: # elif self.accountingType == "GJ":
            # usually is just materials
            self.isVendor = True
            self.sourceID = self.accountingType
            self.sourceName = self.vendorCostDesc
            #self.sourceID = "GJ"
            #self.sourceName = "General Allocation"
        self.date = datetime.strptime(csvRow[offsetCol + 7], '%m/%d/%y')
        self.units = float(csvRow[offsetCol + 14])
        self.cost = float(csvRow[offsetCol + 15].replace(",", ""))
        self.poNumber = csvRow[offsetCol + 10]
        self.invNumber = csvRow[offsetCol + 13]
        self.isPM = self.determineProjectManager()

    def pullInFromSQLRow(self, sqlRow):
        self.projectId = sqlRow[0].strip()
        self.projectDesc = sqlRow[1].strip()
        self.taskId = sqlRow[2].strip()
        self.taskDesc = sqlRow[3].strip()
        self.costType = sqlRow[4].strip()
        self.accountingType = sqlRow[10].strip()
        empId = sqlRow[5].strip()
        vendorId = sqlRow[6].strip()
        if len(empId) > 0:
            self.sourceID = empId # Employee id
            self.sourceName = sqlRow[8].strip()  # Employee name
            self.isVendor = False
        elif len(vendorId) > 0:
            self.sourceID = vendorId  # Vendor id
            self.sourceName = sqlRow[7].strip()  # Vendor name
            self.isVendor = True
        elif self.accountingType == "GJ":
            self.sourceID = "GJ"
            self.sourceName = "General Allocation"
            self.isVendor = False
        elif self.costType == "PTI":
            self.sourceID = "PTI"
            self.sourceName = "Payroll Taxes & Insurance"
            self.isVendor = False
        else:
            self.isVendor = True
            self.sourceID = self.accountingType
            self.sourceName = self.vendorCostDesc
        self.date = sqlRow[9]
        self.vendorCostDesc = sqlRow[12].strip()
        self.units = sqlRow[14]
        self.cost = sqlRow[15]
        self.poNumber = sqlRow[11].strip()
        self.invNumber = sqlRow[13].strip()
        self.projectManagerId = sqlRow[16].strip()
        self.seniorPMId = sqlRow[17].strip()
        self.isPM = self.determineProjectManager()

    def getCostGroupID(self):
        if self.isVendor:
            # specify task code and PO number, in case same place is gone to on the same day
            return "{0} {1} {2} {3}".format(self.sourceID, self.date, self.taskId, self.poNumber)
        else:
            return self.sourceID + " " + str(self.date) # don't specify task code, since vehicle charges go into another task code

    def __str__(self):
        return "Name of {0} (id: {1}, {2}) created ${3} of cost on task {4} on {5}.".format(self.sourceName, self.sourceID,
                "vendor" if self.isVendor else "not a vendor", round(self.cost, 2), self.taskId, self.date)

    def determineProjectManager(self):
        craft = self.taskId[0]
        projectManagerCrafts = ["9"]
        if self.isVendor:
            return False
        elif self.sourceID == self.projectManagerId or self.sourceID == self.seniorPMId:
            return True
        elif craft in projectManagerCrafts and not (self.taskId == "9-94-PTI"):
            return True
        else:
            return False

class dynamicsCondensedData:
    def __init__(self):
        self.groupLookup = {}

    def addCostToGroup(self, newCostRow):
        assert isinstance(newCostRow, dynamicsCost), "The cost group input is not of correct type ('{0}' given)".format(type(newCostRow))
        groupID = newCostRow.getCostGroupID()
        if groupID not in self.groupLookup:
            self.groupLookup[groupID] = dynamicsCostGroup()
        self.groupLookup[groupID].addInCostRow(newCostRow)



class dynamicsCostGroup:
    def __init__(self, initialCostRow=None):
        self.childrenCosts = []
        self.totalCost = 0
        self.projectId = ""
        self.isVendor = False
        self.sourceID = ""
        self.sourceName = ""
        self.taskCode = ""
        self.date = None
        self.poNumber = ""

        if initialCostRow is not None:
            self.addInCostRow(initialCostRow)

    def addInCostRow(self, newCostRow):
        if newCostRow not in self.childrenCosts:
            assert isinstance(newCostRow, dynamicsCost), "The cost group input is not of correct type ('{0}' given)".format(type(newCostRow))
            if len(self.childrenCosts) == 0:
                self.projectId = newCostRow.projectId
                self.isVendor = newCostRow.isVendor
                self.sourceID = newCostRow.sourceID
                self.sourceName = newCostRow.sourceName
                self.taskCode = newCostRow.taskId
                self.date = newCostRow.date
                self.poNumber = newCostRow.poNumber
            self.childrenCosts.append(newCostRow)
            self.totalCost += newCostRow.cost

    def __str__(self):
        return "Name of {0} (id: {1}, {2}) created ${3} of cost on {4}.".format(self.sourceName, self.sourceID,
                "vendor" if self.isVendor else "not a vendor", round(self.totalCost, 2), self.date)


# def getDynamicsCosts(projectNum):
#     # somehow grab the job costs
#     if projectNum == "15898999":
#         return getCostObjFromCSV("./1589-8999 NCR Phase 2/1589-8999 NCR Phase 2.csv")
#     elif projectNum == "71409000":
#         return getCostObjFromCSV("./7140-9000 B&G Headquarters/7140-9000 B&G Headquarters.csv")
#     elif projectNum == "18519000":
#         return getCostObjFromCSV("./1851-9000 Jackson Healthcare/1851-9000 Jackson Healthcare.csv")
#     else:
#         return []

def getCostObjFromCSV(filename=None):
    if filename is not None:
        with open(filename) as fh:
            costCSVReader = csvReader(fh)
            allCostData = []
            havePassedFirstRow = False
            for currCostRow in costCSVReader:
                if havePassedFirstRow:
                    allCostData.append(dynamicsCost(currCostRow))
                else:
                    havePassedFirstRow = True
            return allCostData
    else:
        return []


if __name__ == "__main__":
    costHanlder = dynamicsProjectsCost()

