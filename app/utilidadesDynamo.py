import boto3
import uuid
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime

"""
Lista de Metodos
    traerUUIDConcurso(urlConcurso)
    traerInfoConcurso(urlConcurso)
    insertarConcurso(infoConcurso)
    actualizarConcurso(uidConcurso,infoConcurso)
    eliminarConcurso(uid)

    traerVocesConcurso(urlConcurso)
    insertarVoz(uidConcurso, datosVoz)
    actualizarVoz(uidvoz,uidConcurso,datosVoz)
"""

# Constantes
dynamodb = boto3.resource('dynamodb')
TABLE = dynamodb.Table('ConcursosYVoces')

# Seccion de concurso
def traerUUIDConcurso(urlConcurso):
    response = TABLE.scan(
        FilterExpression=Attr('url_concurso').eq(urlConcurso)
    )
    items = response['Items']
    if len(items) > 0:
        return items[0]["PK"].replace("CON#","")

def traerConcursosUsuario(email_admin):
    response = TABLE.scan(
        FilterExpression=Attr('email_admin').eq(email_admin)
    )
    items = response['Items']
    return items

def traerInfoConcurso(urlConcurso):
    uid = traerUUIDConcurso(urlConcurso)
    pk = 'CON#{}'.format(uid)
    sk = 'METADATA#{}'.format(uid)
    response = TABLE.query(
        KeyConditionExpression=Key('PK').eq(pk) & Key('SK').eq(sk)
    )
    items = response['Items']
    if len(items) > 0:
        return items[0]

def insertarConcurso(nombre,url_imagen,url_concurso,fecha_inicio,fecha_fin,fecha_creacion,valor_pago,guion_voz,recomendaciones,email_admin):
    uid = str(uuid.uuid4())
    return actualizarConcurso(uid,nombre,url_imagen,url_concurso,fecha_inicio,fecha_fin,fecha_creacion,valor_pago,guion_voz,recomendaciones,email_admin)

# Retorna un diccionario con dos keys: Attributes y ResponseMetadata. Attributes trae el objeto despues de ser actualizado
def actualizarConcurso(uid,nombre,url_imagen,url_concurso,fecha_inicio,fecha_fin,fecha_creacion,valor_pago,guion_voz,recomendaciones,email_admin):
    pk = 'CON#{}'.format(uid)
    sk = 'METADATA#{}'.format(uid)

    updatedElement = TABLE.update_item(
    Key={
        'PK': pk,
        'SK': sk
    },
    UpdateExpression='SET nombre = :nombre,url_imagen = :url_imagen,url_concurso = :url_concurso,fecha_inicio = :fecha_inicio,fecha_fin = :fecha_fin,fecha_creacion = :fecha_creacion,valor_pago = :valor_pago,guion_voz = :guion_voz,recomendaciones = :recomendaciones,email_admin = :email_admin',
    ExpressionAttributeValues={
        ':nombre': nombre,
        ':url_imagen':url_imagen,
        ':url_concurso':url_concurso,
        ':fecha_inicio':fecha_inicio,
        ':fecha_fin':fecha_fin,
        ':fecha_creacion':fecha_creacion,
        ':valor_pago':valor_pago,
        ':guion_voz':guion_voz,
        ':recomendaciones':recomendaciones,
        ':email_admin':email_admin
    },
    ReturnValues="ALL_NEW"
    )
    return updatedElement

def actualizarConcursoForm(uid,nombre,url_concurso,fecha_inicio,fecha_fin,valor_pago,guion_voz,recomendaciones):
    pk = 'CON#{}'.format(uid)
    sk = 'METADATA#{}'.format(uid)

    updatedElement = TABLE.update_item(
    Key={
        'PK': pk,
        'SK': sk
    },
    UpdateExpression='SET nombre = :nombre,url_concurso = :url_concurso,fecha_inicio = :fecha_inicio,fecha_fin = :fecha_fin,valor_pago = :valor_pago,guion_voz = :guion_voz,recomendaciones = :recomendaciones',
    ExpressionAttributeValues={
        ':nombre': nombre,
        ':url_concurso':url_concurso,
        ':fecha_inicio':fecha_inicio,
        ':fecha_fin':fecha_fin,
        ':valor_pago':valor_pago,
        ':guion_voz':guion_voz,
        ':recomendaciones':recomendaciones
    },
    ReturnValues="ALL_NEW"
    )
    return updatedElement

# Retorna un diccionario con dos keys: Attributes y ResponseMetadata. Si no elimina nada, no viene Attributes
def eliminarConcurso(uid):
    pk = 'CON#{}'.format(uid)
    sk = 'METADATA#{}'.format(uid)
    response = TABLE.delete_item(
        Key={
        'PK': pk,
        'SK': sk
        },
        ReturnValues="ALL_OLD"
    )
    return response

# Seccion de voces

def traerVocesConcurso(urlConcurso):
    uid = traerUUIDConcurso(urlConcurso)
    pk = 'CON#{}'.format(uid)
    sk = 'VOZ#'
    response = TABLE.query(
        KeyConditionExpression=Key('PK').eq(pk) & Key('SK').begins_with(sk)
    )
    items = response['Items']
    return items

def insertarVoz(uidConcurso,email,nombre,apellido,fecha_creacion,procesado,url_voz_original,url_voz_convertida,observaciones):
    uid = str(uuid.uuid4())
    return actualizarVoz(uid,uidConcurso,email,nombre,apellido,fecha_creacion,procesado,url_voz_original,url_voz_convertida,observaciones)

# Retorna un diccionario con dos keys: Attributes y ResponseMetadata. Attributes trae el objeto despues de ser actualizado
def actualizarVoz(uid,uidConcurso,email,nombre,apellido,fecha_creacion,procesado,url_voz_original,url_voz_convertida,observaciones):
    pk = 'CON#{}'.format(uidConcurso)
    sk = 'VOZ#{}'.format(uid)

    updatedElement = TABLE.update_item(
    Key={
        'PK': pk,
        'SK': sk
    },
    UpdateExpression='SET email = :email,nombre = :nombre,apellido = :apellido,fecha_creacion = :fecha_creacion,procesado = :procesado,url_voz_original = :url_voz_original,url_voz_convertida = :url_voz_convertida,observaciones = :observaciones',
    ExpressionAttributeValues={
        ':email': email,
        ':nombre': nombre,
        ':apellido': apellido,
        ':fecha_creacion': fecha_creacion,
        ':procesado': procesado,
        ':url_voz_original': url_voz_original,
        ':url_voz_convertida': url_voz_convertida,
        ':observaciones': observaciones
    },
    ReturnValues="ALL_NEW"
    )
    return updatedElement

#print(insertarConcurso("El precio es correcto","","elprecioescorrecto",datetime.today().strftime('%Y-%m-%d'),datetime.today().strftime('%Y-%m-%d'),datetime.today().strftime('%Y-%m-%d-%H:%M:%S'),0,"","","kevin.infante2@gmail.com"))
#uid = traerUUIDConcurso("lavozkids")
#print(insertarVoz(uid,"kevininhe@outlook.com","Carlos","Pelaez",datetime.today().strftime('%Y-%m-%d'),False,"","","observaciones"))
#print(eliminarConcurso(uid))
#actualizarConcurso(uid,"La voz Senior Revenge","","lavozsenior",datetime.today().strftime('%Y-%m-%d'),datetime.today().strftime('%Y-%m-%d'),datetime.today().strftime('%Y-%m-%d-%H:%M:%S'),0,"","","kevin.infante2@gmail.com")
#print(traerVocesConcurso("lavozkids"))