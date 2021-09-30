from utils import sql
import logging

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    dimension = req.params.get('dimension')

    if dimension == 'customer':
        # TODO: get unique list of customers from Nulogy
        # TODO: update azure warehouse customer dimension table
        pass
    elif dimension == 'warehouse':
        # TODO: implement warehouse update process
        pass



    
    return func.HttpResponse(status_code=200)

