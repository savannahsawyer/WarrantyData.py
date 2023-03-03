warrantyAPIKey = {
    'access_token': '3zolofrfk5ww2plarjdo5lku8y',
    'user_agent': 'McK Energy Services',
    'sheet_id': 1858531671467908,
    'sheet_hyperlink': 'https://app.smartsheet.com/sheets/5R9QqvJv3HJg94FW9WxjHQQmRWm47r9G2Q6P2PG1',
}

"""Smartsheet logic

Handles pushing data to Smartsheet.

"""

from smartsheet import Smartsheet
from smartsheet.sheets import Sheets
from smartsheet.models.sheet import Sheet as SmartsheetSheet
from smartsheet.models.cell import Cell as SmartsheetCell
from smartsheet.models.row import Row as SmartsheetRow
from smartsheet.models.column import Column as SmartsheetColumn
from datetime import datetime

class smartsheetConnection:

    def __init__(self, access_token, user_agent, appVersion=None):
        self.access_token = access_token
        self.user_agent = user_agent
        self.appVersion = appVersion
        self.apiModel = None
        self.sheets = None
        self.createConnection()

    def createConnection(self):
        self.apiModel = Smartsheet(access_token=self.access_token, user_agent=self.user_agent)
        self.apiModel.errors_as_exceptions(True)
        self.sheets = Sheets(self.apiModel)

    def getSheet(self, sheetID, page_size=5000):
        print("Accessing sheet with ID '{0}'".format(sheetID))
        sheet = self.sheets.get_sheet(sheetID, page_size=page_size)
        print("Sheet '{0}' accessed for Smartsheet".format(sheet.name))
        return sheet # create an object for this?

import pickle
class warrantyCall:
    allCalls = []
    projectCallLookup = {}
    rowsToUpdate = []
    smConn = None
    warrantySheet = None
    column_map = {}

    @classmethod
    def getWarrantySheet(cls):
        if cls.warrantySheet is None:
            if cls.smConn is None:
                cls.smConn = smartsheetConnection(warrantyAPIKey["access_token"], warrantyAPIKey["user_agent"])
            cls.warrantySheet = cls.smConn.getSheet(warrantyAPIKey["sheet_id"])
            cls.createColumnLookup()
        return cls.warrantySheet

    @classmethod
    def pushUpdateRows(cls):
        cls.getWarrantySheet() # ensure that have a sheet & connection
        print("Initializing update for the estimated costs")
        assert isinstance(cls.smConn, smartsheetConnection), "The Smartsheet connector (type '{0}') is not the correct type".format(type(cls.smConn))
        assert isinstance(cls.warrantySheet, SmartsheetSheet), "Input warranty sheet (type '{0}') is not the correct type".format(type(cls.warrantySheet))
        if len(cls.rowsToUpdate) > 0:
            result = Sheets.update_rows(cls.smConn.sheets, cls.warrantySheet.id_, cls.rowsToUpdate)
            print("Completed cost push, affecting {0} rows".format(len(cls.rowsToUpdate)))
            return result
        else:
            print("No rows required an update")
            return None

    @classmethod
    def getWarrantyCalls(cls):
        if len(cls.allCalls) == 0:
            # means that there are no calls, so go ahead an pull
            cls.getWarrantySheet() # pulls in the smartsheet and connection
            assert isinstance(cls.warrantySheet, SmartsheetSheet), "Input warranty sheet (type '{0}') is not the correct type".format(type(cls.warrantySheet))
            for currRow in cls.warrantySheet.rows:
                newCall = warrantyCall(currRow)
                cls.allCalls.append(newCall)
                if len(newCall.jobNumber) > 0:
                    if newCall.jobNumber not in cls.projectCallLookup:
                        cls.projectCallLookup[newCall.jobNumber] = []
                    cls.projectCallLookup[newCall.jobNumber].append(newCall)
        return cls.allCalls

    @classmethod
    def pickleWarrantyCalls(cls):
        with open("allcalls.info", mode="wb") as fh:
            pickle.dump(cls.allCalls, fh)

    @classmethod
    def unpickleWarrantyCalls(cls):
        with open("allcalls.info", mode="rb") as fh:
            cls.allCalls = pickle.load(fh)
        for currCall in cls.allCalls:
            if len(currCall.jobNumber) > 0:
                if currCall.jobNumber not in cls.projectCallLookup:
                    cls.projectCallLookup[currCall.jobNumber] = []
                cls.projectCallLookup[currCall.jobNumber].append(currCall)

    @classmethod
    def createColumnLookup(cls):
        if len(cls.column_map) == 0:
            assert isinstance(cls.warrantySheet, SmartsheetSheet), "Input warranty sheet (type '{0}') is not the correct type".format(type(cls.warrantySheet))
            for column in cls.warrantySheet.columns:
                assert isinstance(column, SmartsheetColumn), "Input warranty column (type '{0}') is not the correct type".format(type(column))
                cls.column_map[column.title] = column.id_
        return cls.column_map




    def __init__(self, sourceRow):
        self.sourceRow = sourceRow
        self.jobNumber = self.cleanProjectNumber(self.getCellValueFromColumnName("Job Number"))
        self.taskCodes = self.getTaskCodes()
        self.createDate = self.getSmartsheetDate()
        self.issueDesc = self.getCellValueFromColumnName("Issue")
        self.callerName = self.getCellValueFromColumnName("Caller")
        self.respondingTech = self.getCellValueFromColumnName("Responding Technician")
        self.issueFound = self.getCellValueFromColumnName("Issue Found")
        self.PONum = self.getPONum()
        self.costCell = self.getCellByColumnName("Estimated Cost")
        self.calcCost = 0

    def getSmartsheetDate(self):
        rowCreatedDate = self.getCellValueFromColumnName("Created")
        callCreatedDate = self.getCellValueFromColumnName("Created Date")
        if len(rowCreatedDate) == 0:
            return None
        if len(callCreatedDate) > 0:
            return datetime.strptime(callCreatedDate, '%Y-%m-%d')
        else:
            return datetime.strptime(rowCreatedDate, '%Y-%m-%dT%H:%M:%SZ')  # will be like 2018-03-07T15:37:05Z

    def getPONum(self):
        # grab the PO number if it exists
        poNum = self.getCellValueFromColumnName("PO #'s")
        if poNum is None:
            return None
        elif len(poNum) == 0:
            return None
        poNum = ("0000" + str(poNum))[-4:] # take the last four digits once it gets stripped
        return poNum


    def getTaskCodes(self):
        # used to be this: str(self.getCellValueFromColumnName("Task Code"))[0:8].replace("-", "")
        # grabs all of the task codes from the row
        taskCodeStr = str(self.getCellValueFromColumnName("Task Code")).strip()

        if len(taskCodeStr) == 0 or taskCodeStr == "None":
            return []

        # first, get rid of all dashes
        taskCodeStr = taskCodeStr.replace("-", "")

        # next, convert all space separators to slashes
        taskCodeStr = taskCodeStr.replace(" ", "/")

        # now replace all repetitive slashes with one slash
        while "//" in taskCodeStr:
            taskCodeStr = taskCodeStr.replace("//", "/")

        # next, split up the task codes, and only take the first 6 characters of each
        taskCodeList = [a[0:6] for a in taskCodeStr.split("/")]
        return taskCodeList

    def getCellByColumnName(self, columnName):
        colID = warrantyCall.column_map[columnName]
        return self.sourceRow.get_column(colID)

    def getCellValueFromColumnName(self, columnName):
        associatedCell = self.getCellByColumnName(columnName)
        assert isinstance(associatedCell, SmartsheetCell), "Cell is not of correct type"
        cellVal = associatedCell.value
        if cellVal is None:
            return ""
        else:
            return cellVal

    def cleanProjectNumber(self, jobNumber):
        return str(jobNumber).replace(" ", "").replace("-", "")[:8]

    def pushCost(self, newCalcCost = None):
        if newCalcCost is not None:
            self.calcCost = newCalcCost

        newCostValue = round(self.calcCost, 2)
        oldCostValue = self.costCell.value
        if newCostValue != oldCostValue:
            print("Discrepancy found: replacing cost of {0} with cost of {1}".format(oldCostValue, newCostValue))
            assert isinstance(self.costCell, SmartsheetCell), "Input warranty cell (type '{0}') is not the correct type".format(type(self.costCell))
            assert isinstance(self.sourceRow, SmartsheetRow), "Input warranty row (type '{0}') is not the correct type".format(type(self.sourceRow))
            newCostCell = SmartsheetCell()
            newCostCell.column_id = self.costCell.column_id # need to set this up
            newCostCell.value = newCostValue
            rowWithUpdateInfo = SmartsheetRow()
            rowWithUpdateInfo.id = self.sourceRow.id
            rowWithUpdateInfo.cells.append(newCostCell)
            warrantyCall.rowsToUpdate.append(rowWithUpdateInfo)

