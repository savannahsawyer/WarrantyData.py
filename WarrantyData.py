import csv
import datetime
from SmartsheetIntegrator import warrantyCall
from DynamicsIntegrator import dynamicsCostGroup, dynamicsProjectsCost
import logging

def getCallDataFromCSV(filename=None):
    if filename is not None:
        with open(filename) as fh:
            callsCSVReader = csv.reader(fh)
            allCallData = []
            havePassedFirstRow = False
            for currCallRow in callsCSVReader:
                if havePassedFirstRow:
                    # grab the information from the row
                    allCallData.append([currCallRow[2], # task code
                                        datetime.datetime.strptime(currCallRow[4], '%m/%d/%y %H:%M %p'), # date
                                        currCallRow[5], # issue
                                        currCallRow[7], # caller
                                        currCallRow[8], # Responding tech
                                        currCallRow[9],
                                        0, # total cost associated with the call
                                        currCallRow] # save off all information for the call, so that can be saved later
                                       ) # issue found
                else:
                    havePassedFirstRow = True
            return allCallData
    else:
        return []

def nameMatch(callName, costName):
    nameParts = callName.rsplit(' ', 1)
    if len(nameParts) < 2: # need at least a first and last name
        return False
    if nameParts[1] in costName: # see if the last name is in the name of the entity that created the cost
        return True
    else:
        return False

def getDaysDifference(date1, date2):
    dateDiff = date1 - date2
    return abs(dateDiff.days + dateDiff.seconds/86400)


def ratingFunction(callInfo, costCode):
    #rates similarity of cost group to call
    # cost code will either be an employee labor cost, and look like this: [task code, employee ID, date of labor, total cost of labor]
    #       Sample: ['2-10-100- DOAS Units', 'Jesse Lee Gentes', datetime.datetime(2019, 2, 4, 0, 0), 232.36]
    #   or a subcontract cost, and look like this: [Task code, vendor name, date of cost, PO number, PO description, and total cost]
    #       Sample: ['2-10-100- DOAS Units', 'Home Depot Inc', datetime.datetime(2019, 3, 5, 0, 0), '2', '6532/3514408', 227.93]
    # call code has information from a call, and will look like this:
    assert isinstance(callInfo, warrantyCall), "Incorrect type used for a warranty type (given type '{0}')".format(type(callInfo))
    assert isinstance(costCode, dynamicsCostGroup), "Condensed data is incorrect type; '{0}' given.".format(
        type(costCode))
    ratingNum = 0

    if nameMatch(callInfo.respondingTech, costCode.sourceName):
        ratingNum += 20

    # for each (or any) of the task codes, find the one that matches best to the cost,
    # and assign weight based on how well it matches
    maxTaskContribution = 0
    for taskCode in callInfo.taskCodes:
        if costCode.taskCode == taskCode: # try to match the task code exactly
            maxTaskContribution = max(maxTaskContribution, 50)
        elif costCode.taskCode[:3] == taskCode[:3]: # try to match the task group
            maxTaskContribution = max(maxTaskContribution, 35)
        elif costCode.taskCode[:1] == taskCode[:1]: # try to match the craft
            maxTaskContribution = max(maxTaskContribution, 15)
    ratingNum += maxTaskContribution

    # add in weights if the PO number matches
    if callInfo.PONum == costCode.poNumber:
        print("Found a match between PO numbers {0} and {1}".format(callInfo.PONum, costCode.poNumber))
        ratingNum += 100

    # add a higher weight for closer days, and penalize name & task code matches for being further away from the date
    daysDiff = getDaysDifference(costCode.date, callInfo.createDate)
    if daysDiff < 1:
        ratingNum *= 1
        ratingNum += 35
    elif daysDiff < 3:
        ratingNum *= 0.8
        ratingNum += 25
    elif daysDiff < 7:
        ratingNum *= 0.5
        ratingNum += 10
    elif daysDiff < 14:
        ratingNum *= 0.25
        ratingNum += 5
    elif daysDiff > 90:
        # if more than half a year ago, then disregard
        ratingNum = -1 # smaller than the minimum possible rating, so will get thrown out

    return ratingNum

def chooseCallsToApplyCostTo(costRow, callsList): #cost row is one row of grouped costs
    #sort through costs to apply to calls
    bestRatingSoFar = 0 # note that this matches the minimum possible rating
    bestCorrespondingCalls = []
    for currCallRow in callsList:
        # figure out if this current call row is better than the previous best call to be found
        rating = ratingFunction(currCallRow, costRow)
        if rating > bestRatingSoFar:
            bestRatingSoFar = rating
            bestCorrespondingCalls = [currCallRow]
        elif rating == bestRatingSoFar:
            bestCorrespondingCalls.append(currCallRow)

    return bestCorrespondingCalls

def correspondCostsToCalls(condensedCostList, callList):
    unmatchedCosts = 0
    for costRow in condensedCostList:
        assert isinstance(costRow, dynamicsCostGroup), "Condensed data is incorrect type; '{0}' given.".format(type(costRow))
        # find a list of rows of the call list that is the best match
        mostLikelyCallMatches = chooseCallsToApplyCostTo(costRow, callList)
        if len(mostLikelyCallMatches) > 0:
            costPerCall = costRow.totalCost / len(mostLikelyCallMatches)
            for matchingCall in mostLikelyCallMatches:
                assert isinstance(matchingCall, warrantyCall), "Incorrect type given ('{0}')".format(type(matchingCall))
                matchingCall.calcCost += costPerCall
        else:
            # no calls matched - note that this means that probably shouldn't match to any calls
            unmatchedCosts += costRow.totalCost
    return unmatchedCosts

def runCostAssociationsForWarrantyData():
    costHanlder = dynamicsProjectsCost()
    warrantyCall.getWarrantyCalls()  # pull in all of the warranty calls from the smartsheet
    for projectNum, projectCalls in warrantyCall.projectCallLookup.items():
        # condense down the costs into their main components, and then spread the PM time evenly over all calls
        condensedCost, PMTotalCost = costHanlder.getCondensedJobCosts(projectNum)

        if len(condensedCost) > 0:
            print("Performing data matching for project {0} with {1} calls".format(projectNum, len(projectCalls)))

            # next, associate the costs with each of the calls
            unmatchedCosts = correspondCostsToCalls(condensedCost, projectCalls)

            # finally, incorporate in the remaining costs and then request the cost update
            unaccountedCostPerCall = (PMTotalCost) / len(projectCalls) # take out the unmatched costs part
            for currCall in projectCalls:
                assert isinstance(currCall, warrantyCall), "Input warranty call (type '{0}') is not the correct type".format(type(currCall))
                currCall.calcCost += unaccountedCostPerCall
                currCall.pushCost() # push all of the costs (this will figure out which rows need to be updated)
        else:
            print("No costs found for project '{0}'".format(projectNum))
    warrantyCall.pushUpdateRows() # push all cost updates to the smartsheet

def logger():
    out = r'\\mckatlauto2\Users\rpasvc1\Documents\Lloyd\WarrantyCostsAssociation\app.log'
    logging.basicConfig(filename=out, filemode='w', format='%(name)s - %(levelname)s - %(message)s')
    logging.warning('Success')
if __name__ == "__main__":
    #warrantyCall.getWarrantyCalls()
    #warrantyCall.pickleWarrantyCalls()
    #warrantyCall.unpickleWarrantyCalls()
    runCostAssociationsForWarrantyData()
    logger()
