import requests
import json
import re
import time
import ssl
from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings

disable_warnings(InsecureRequestWarning)

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Python herdado que não verifica certificados HTTPS por padrão
    pass
else:
    # Lidar com o ambiente de destino que não oferece suporte à verificação de HTTPS
    ssl._create_default_https_context = _create_unverified_https_context

# Documentação sobre uso da API do pipefy - https://pipefypipe.docs.apiary.io/
# https://api-docs.pipefy.com/reference/query/overview/


### Inicio Constantes
with open("""python/apis/pipefy/.credentials/credentials.json""") as f:
    acessos=json.load(f) 
authKey = acessos['authKey']
organizationId = acessos['organizationId']

results = {
    'phases' : '''{ phases
                {
                id
                name
                description
                cards_count
                expiredCardsCount
                color
                created_at
                done
                sequentialId
                fields {
                    id
                    label
                    type
                    description
                    editable
                    help
                    index_name
                    is_multiple
                    minimal_view
                    required
                    synced_with_card
                    options
                    canConnectExisting
                    canConnectMultiples
                    canCreateNewConnected
                    connectedRepo
                    custom_validation
                    childMustExistToFinishParent
                    allChildrenMustBeDoneToFinishParent
                    allChildrenMustBeDoneToMoveParent   
                }
                fieldConditions {
                    id
                    name
                    phase {
                        id
                        name
                    }
                    condition {
                        id
                        expressions {
                            id
                            field_address
                            operation
                            value
                            structure_id
                        }
                        expressions_structure
                        related_cards {
                            id
                        }
                    }
                    actions {
                        id
                    }
                } 
            }
        }''',
    'users' : '''{
                    users {
                        id
                        username
                        name
                        displayName
                        email
                        preferences {
                            browserNativeNotificationEnabled
                            displayImprovements
                            displayOrganizationReportSidebar
                            displayPipeReportsSidebar
                            suggestedTemplatesClosed
                            useNewOpenCard
                        }
                    }
                }''', 
    'labels' : '{labels{id name color}}', 
    'pipe' : '''
        {
            name
            cards_count
            users {
                id
                username
                name
                displayName
                email
                preferences {
                    browserNativeNotificationEnabled
                    displayImprovements
                    displayOrganizationReportSidebar
                    displayPipeReportsSidebar
                    suggestedTemplatesClosed
                    useNewOpenCard
                }
                timeZone
                locale
            }
            labels {
                color
                name
                id
            }
            start_form_fields {
                id
                label
                type
                description
                editable
                help
                index_name
                is_multiple
                minimal_view
                required
                synced_with_card
                options
                canConnectExisting
                canConnectMultiples
                canCreateNewConnected
                connectedRepo
                custom_validation
                childMustExistToFinishParent
                allChildrenMustBeDoneToFinishParent
                allChildrenMustBeDoneToMoveParent  
            }
            startFormFieldConditions {
                id
                name
                phase {
                    id
                    name
                }
                condition {
                    id
                    expressions {
                        id
                        field_address
                        operation
                        value
                        structure_id
                    }
                    expressions_structure
                    related_cards {
                        id
                    }
                }
                actions {
                    id
                }
            } 
            phases {
                id
                name
                description
                cards_count
                expiredCardsCount
                color
                created_at
                done
                sequentialId
                fields {
                    id
                    label
                    type
                    description
                    editable
                    help
                    index_name
                    is_multiple
                    minimal_view
                    required
                    synced_with_card
                    options
                    canConnectExisting
                    canConnectMultiples
                    canCreateNewConnected
                    connectedRepo
                    custom_validation
                    childMustExistToFinishParent
                    allChildrenMustBeDoneToFinishParent
                    allChildrenMustBeDoneToMoveParent   
                }
                fieldConditions {
                    id
                    name
                    phase {
                        id
                        name
                    }
                    condition {
                        id
                        expressions {
                            id
                            field_address
                            operation
                            value
                            structure_id
                        }
                        expressions_structure
                        related_cards {
                            id
                        }
                    }
                    actions {
                        id
                    }
                } 
            }
        }'''
    ,'cards' : '''
        {
            pageInfo {
                startCursor  
                endCursor
                hasPreviousPage
                hasNextPage
            }
            edges {        
                node {'''
    ,'cardsEnd': r'}}}'
    ,'nodeAttributeSameName' : ('age', 'attachments_count', 'checklist_items_checked_count', 'checklist_items_count', 'comments_count', 'createdAt', 'creatorEmail', 'current_phase_age', 'done', 'due_date', 'emailMessagingAddress', 'expired', 'finished_at', 'id', 'late', 'path', 'started_current_phase_at', 'suid', 'title', 'updated_at', 'url', 'uuid')
    ,'nodeAttribute' : {
            'assignees' : '''assignees {
                                id
                                name
                            }
                            cardAssignees {
                                id
                                assignedAt
                            }\n'''
            ,'attachments' : '''attachments {
                                createdAt
                                path
                                url
                                createdBy {
                                    id
                                }
                                field {
                                    id
                                }
                            }\n'''
            ,'child_relations' : '''child_relations {
                                id
                                cards {
                                id
                                }
                                name
                                pipe {
                                id
                                }
                                repo
                                source_type
                            }\n'''
            ,'comments' : ''' comments {
                                id
                                author {
                                    id
                                }
                                created_at
                                text
                            }\n'''
            ,'createdBy' : ''' createdBy {
                                id
                                created_at
                                name   
                            }\n'''
            ,'currentLateness' : ''' currentLateness {
                                id
                                becameLateAt
                                shouldBecomeLateAt
                                sla
                            }\n'''
            ,'current_phase' : '''current_phase {
                                id
                                name
                                cards_can_be_moved_to_phases {
                                    id
                                    name
                                }
                                cards_count
                                lateCardsCount
                            }\n'''
            ,'expiration' : '''expiration {
                                expiredAt
                                shouldExpireAt
                            }\n'''
            ,'fields' : '''fields {
                                field {
                                    id
                                }
                                date_value
                                datetime_value
                                filled_at
                                float_value
                                indexName
                                name
                                report_value
                                updated_at
                                value
                            }\n'''
            ,'inbox_emails' : '''inbox_emails {
                                id
                            }\n'''
            ,'labels' : '''  labels {
                                id
                                color
                                name
                            }\n'''
            ,'parent_relations' : '''parent_relations {
                                id
                                cards {
                                id
                                }
                                pipe {
                                id
                                }
                                source_type
                            }\n'''
            ,'phases_history' : '''phases_history {
                                phase {
                                    id
                                    name
                                    }
                                became_late
                                created_at
                                draft
                                duration
                                firstTimeIn
                                lastTimeIn
                                lastTimeOut
                            }\n'''
            ,'subtitles' : '''subtitles {
                                date_value
                                datetime_value
                                filled_at
                                float_value
                                indexName
                                name
                                report_value
                                updated_at
                                value
                            }\n'''
            ,'summary' : '''summary {
                                title
                                type
                                value
                            }\n'''
            ,'summary_attributes' : '''summary_attributes {
                                title
                                type
                                value
                            }\n'''
            ,'summary_fields' : '''summary_fields {
                                title
                                type
                                value
                            }\n'''
    }
}
### Fim Constantes

def getCardAttributes(attributes, hasPageInfo = True):
    

    data = str()
    try:
        for att in attributes:
            if att in results['nodeAttributeSameName']:
                data += '{}\n'.format(att)
            else:
                data += results['nodeAttribute'][att]
    except:
        raise Exception("Attribute doesn't exist.")
    return results['cards'] + data + results['cardsEnd'] if hasPageInfo else '{' + data + '}'



def send(query, type = 'post', url = 'https://app.pipefy.com/queries', headers = {'Content-Type': 'application/json','Authorization': 'Bearer ' + authKey}, returnContentJson = True):
    
    
    erro, tentativas = None, 0
    while not erro:
        try:
            with requests.Session() as session:
                if type == 'post':
                    request = session.post(url, json={'query': query}, headers=headers)
                    result = json.loads(request.content) if returnContentJson else request
                elif type == 'put':
                    request = session.put(url, headers=headers,data=query)
                    result = request
                return result
        except Exception as e:
            erro = str(e)
            if re.search(r'(?i)(time.+out|connect.+abort|max.+retr.+exce)',erro):
                tentativas += 1
                if tentativas >= 100:
                    raise Exception('Requisição foi repetida 100 vezes sem sucesso! \n\n {}'.format(erro))
                erro = None
                time.sleep(1)
            else:
                raise e


def dictToJson(inputDict, returnAsDict = True):
    j = re.sub(r'(?<!: )\"(\S*?)\"', r'\1', json.dumps(inputDict))
    j = re.sub(r'(operator.+?)\"(\w+)\"', r'\1\2', j)
    return j if returnAsDict else j[1:-1]
    
def createComment(cardId, comment):
    
    input = dictToJson({'card_id': cardId,'text': comment})
    query = " mutation {  createComment(input: "+ input +") {comment {id text} }} "
    return send(query)

def deleteCard(cardId):
    query = "mutation{ deleteCard(input: {id: " + cardId + " }) { success } }"
    return send(query)

def showCard(cardId, attributes = ['fields']):
    ''' attributes = ['fields'] : list of attributes from node ex.: ['att1','att2','att3'...]\n
        Possible attributes : 'age','assignees','attachments','attachments_count','checklist_items_checked_count','checklist_items_count','child_relations','comments','comments_count','createdAt','createdBy','creatorEmail','currentLateness','current_phase','current_phase_age','done','due_date','emailMessagingAddress','expiration','expired','fields','finished_at','id','inbox_emails','labels','late','parent_relations','path','phases_history','started_current_phase_at','subtitles','suid','summary','summary_attributes','summary_fields','title','updated_at','url','uuid'
    '''   
    result = getCardAttributes(attributes, False)
    query = "{ card(id: " + cardId + ") " + result + " }"
    return send(query)


def commentsCard(cardId):
    query = "{ card(id: " + cardId + ") { title comments { id text created_at author_name } } }"
    return send(query)


def moveCardToPhase(cardId, phaseId):

    input = dictToJson({'card_id': cardId,'destination_phase_id': phaseId})
    query = "mutation{ moveCardToPhase(input: "+ input +") { card{ id current_phase { name } } } }"
    return send(query)


def updateFields(cardId, fields):
    '''fields:   [['id_campo1', 'valor1'], ['id_campo2', 'valor2']]'''
    fieldsList = [{ 'fieldId': field, 'value': value } for field, value in fields if value]
    input = dictToJson({'nodeId': cardId,'values': fieldsList}) 
    query = "mutation { updateFieldsValues(input: " + input + ") { success userErrors { field message } } }"
    return send(query)


def createCard(pipeId, title, fields, titlePrefix=str(), assignee_ids=list(), label_ids=list()):
    '''fields = list ex.: [[fieldId1, value1],[fieldId2, value2],...]\n
    assignee_ids = list ex.: ['assignee_id1','assignee_id2',...]\n
    label = list ex.: ['label1','label2',...]\n
    '''
    fieldsList = [{'field_id': fieldId, 'field_value': value} for fieldId, value in fields if value]
    input = dictToJson({'pipe_id': pipeId,'title': '{}{}'.format(titlePrefix,title), 'assignee_ids' : assignee_ids , 'label_ids' : label_ids,'fields_attributes' : fieldsList})
    query = "mutation { createCard( input: " + input +" )  { card { id title url } } }"
    return send(query)


def updateAssigneeIds(cardId, assignee_ids):
    

    input = dictToJson({'id': cardId, 'assignee_ids': assignee_ids })
    query = "mutation { updateCard( input: "+ input +") {card {id title} } }"
    return send(query)

def updateLabelIds(cardId, label_ids):
    

    input = dictToJson({'id': cardId, 'label_ids': label_ids })
    query = "mutation { updateCard( input: "+ input +") {card {id title} } }"
    return send(query)


def uploadFile(file_name, file_base64, cardId, field_id, organizationId=organizationId):
    

    input = dictToJson({'fileName': file_name, 'organizationId': organizationId })
    query = "mutation { createPresignedUrl(input: "+ input +")  { clientMutationId downloadUrl url } }"
    r = send(query)
    urlPut  = r['data']['createPresignedUrl']['url']
    urlFile = r['data']['createPresignedUrl']['downloadUrl']
    headers = {'Content-Type': 'application/' + file_name[file_name.rfind('.')+1:]}
    send(file_base64,'put',urlPut,headers)
    arquivo = re.findall(r'(orgs.+)\?', urlFile)[0]
    r = updateFields(cardId, [[field_id, arquivo]])
    return r['data']['updateFieldsValues']['success']


def updateCard(cardId, mutationId):


    input = dictToJson({'id': cardId, 'clientMutationId' : mutationId })
    query = "mutation { updateCard( input: "+ input + ") { card { id title } } }"
    return send(query)

    

def relateCards(parentId, childId, sourceId, sourceType = "PipeRelation"):    
    '''parentId: CardId wich will be the parent of the relation\n
    childId: CardId wich will be the child of the relation \n
    sourceId: Connection ID between the pipes\n
    '''
    
    input = dictToJson({ 'parentId': parentId, 'childId': childId, 'sourceType': sourceType, 'sourceId': sourceId })
    query = "mutation { createCardRelation(input: "+ input +") { cardRelation { id } } } "
    return send(query)


def pipeInfo(pipeId):
    return send('{pipe(id: '+ str(pipeId) +')'+ results['pipe'] +'}')


def pipeLabels(pipeId):
    return send('{pipe(id: '+ str(pipeId) +')'+ results['labels'] +'}')['data']['pipe']['labels']


def pipePhases(pipeId):
    return send('{pipe(id: '+ str(pipeId) +')'+ results['phases'] +'}')['data']['pipe']['phases']


def pipeUsers(pipeId):
    return send('{pipe(id: '+ str(pipeId) +')'+ results['users'] +'}')['data']['pipe']['users']


def searchCards(pipeId, searchTitle, attributes = ['fields'] ):
    ''' searchTitle = str or list - part of name of card title
        attributes = ['fields'] : list of attributes from node ex.: ['att1','att2','att3'...]\n
        Possible attributes : 'age','assignees','attachments','attachments_count','checklist_items_checked_count','checklist_items_count','child_relations','comments','comments_count','createdAt','createdBy','creatorEmail','currentLateness','current_phase','current_phase_age','done','due_date','emailMessagingAddress','expiration','expired','fields','finished_at','id','inbox_emails','labels','late','parent_relations','path','phases_history','started_current_phase_at','subtitles','suid','summary','summary_attributes','summary_fields','title','updated_at','url','uuid'
    '''   
    def getCards(title):

        input = {'pipe_id' : pipeId}
        input['search'] = {'title' : title}
        input = dictToJson(input,False)
        query = '{cards('+ input +') '+ result +'}'
        response = send(query)
        edges = response['data']['cards']['edges']
        if len(edges) > 0:
            cards.extend(node['node'] for node in edges)


    if type(attributes) != list:
        raise Exception('attributes must be a list')
    result = getCardAttributes(attributes)
    cards = list()
    if type(searchTitle) == str:
        getCards(searchTitle)
    elif type(searchTitle) == list:
        [getCards(title) for title in searchTitle]
    else:
        raise Exception('searchTitle must be str or list.')
    return cards


def listAllCards(pipeId, filters = None, attributes = ['fields'], onlyCardsOnPhaseName = None):
    ''' filter = dict - values to be filter ex: {'field' : 'someField', 'value' : 'someValue', operator : 'gt' }\n
        Possible operators: 'equal' = Equals to, 'gt' = Greater than, 'gte' = Greater than or equal to, 'lt' = Less than, 'lte' = Less than or equal to\n
        attributes = ['fields'] : list of attributes from node ex.: ['att1','att2','att3'...]\n
        Possible attributes : 'age','assignees','attachments','attachments_count','checklist_items_checked_count','checklist_items_count','child_relations','comments','comments_count','createdAt','createdBy','creatorEmail','currentLateness','current_phase','current_phase_age','done','due_date','emailMessagingAddress','expiration','expired','fields','finished_at','id','inbox_emails','labels','late','parent_relations','path','phases_history','started_current_phase_at','subtitles','suid','summary','summary_attributes','summary_fields','title','updated_at','url','uuid'
    '''
    if type(attributes) != list:
        raise Exception('attributes must be a list')
    if filters and type(filters) != dict:
        raise Exception('filters must be dict.')
    if onlyCardsOnPhaseName:
        attributes.append('current_phase')
    result = getCardAttributes(attributes) 
    hasNextPage = True
    allCards, after = list(), None
    while hasNextPage:
        input = {'pipeId' : pipeId}
        if filters:
            input['filter'] = filters
        if after:
            input['after'] = after
        input = dictToJson(input,False)
        query = '{allCards('+ input +') '+ result +'}'
        cards = send(query)
        edges = cards['data']['allCards']['edges']
        if len(edges) > 0:
            for node in edges:
                if onlyCardsOnPhaseName and onlyCardsOnPhaseName != node['node']['current_phase']['name']:
                    continue
                allCards.append(node['node'])
            if onlyCardsOnPhaseName and len(allCards) > 0 and onlyCardsOnPhaseName != node['node']['current_phase']['name']:
                break
        after = cards['data']['allCards']['pageInfo']['endCursor']
        hasNextPage = cards['data']['allCards']['pageInfo']['hasNextPage']
    return allCards


def deleteAllCards(pipeId):


    allCards = listAllCards(pipeId, attributes = ['id'])
    total, n = len(allCards), 0
    for card in allCards:
        deleteCard(card['id'])
        n += 1
        if n % 10 == 0:
            print('Deleted {} from {} cards.'.format(n, total))
    print('All {} cards are deleted.'.format(n))
    
def listPhase(phaseId=[], filters = None, attributes = ['fields']):
    ''' filter = dict - values to be filter ex: {'field' : 'someField', 'value' : 'someValue', operator : 'gt' }\n
        Possible operators: 'equal' = Equals to, 'gt' = Greater than, 'gte' = Greater than or equal to, 'lt' = Less than, 'lte' = Less than or equal to\n
        attributes = ['fields'] : list of attributes from node ex.: ['att1','att2','att3'...]\n
        Possible attributes : 'age','assignees','attachments','attachments_count','checklist_items_checked_count','checklist_items_count','child_relations','comments','comments_count','createdAt','createdBy','creatorEmail','currentLateness','current_phase','current_phase_age','done','due_date','emailMessagingAddress','expiration','expired','fields','finished_at','id','inbox_emails','labels','late','parent_relations','path','phases_history','started_current_phase_at','subtitles','suid','summary','summary_attributes','summary_fields','title','updated_at','url','uuid'
    '''
    if type(attributes) != list:
        raise Exception('attributes must be a list')
    if filters and type(filters) != dict:
        raise Exception('filters must be dict.')
    if type(phaseId) != list :
        raise Exception('phaseId not in list')

    allCards=list()
    for p in phaseId:
        
        hasNextPage,after = True,''
        result = getCardAttributes(attributes,hasPageInfo=False)
        result=result[1:-1]

        while hasNextPage:
            query = '{phase(id:'+ str(p) +'){ cards'+ after+'{pageInfo {endCursor startCursor hasNextPage} edges { node { id '+ result +'}}}}}'
            cards = send(query)
            edges = cards['data']['phase']['cards']['edges']
            hasNextPage=cards['data']['phase']['cards']['pageInfo']['hasNextPage']
            endCursor=cards['data']['phase']['cards']['pageInfo']['endCursor']
            after=f"""(after: "{endCursor}")"""
            
            if len(edges) > 0:
                for node in edges:
                    allCards.append(node['node'])
    return allCards
