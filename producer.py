import pika
import json
from mongoengine import connect, Document, StringField, BooleanField
from faker import Faker
from bson import ObjectId

connect('contacts_db')

class Contact(Document):
    full_name = StringField(required=True)
    email = StringField(required=True)
    message_sent = BooleanField(default=False)



def generate_contacts(num_contacts):
    fake = Faker()
    contacts = []
    for _ in range(num_contacts):
        full_name = fake.name()
        email = fake.email()
        contact = Contact(full_name=full_name, email=email)
        contact.save()
        contacts.append(contact)
    return contacts


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='email_queue')

num_contacts_to_generate = 5  
generated_contacts = generate_contacts(num_contacts_to_generate)

for contact in generated_contacts:
    message = {'contact_id': str(contact.id)}
    channel.basic_publish(
        exchange='', routing_key='email_queue', body=json.dumps(message))
    print(f"Sent {message} to the email_queue")

connection.close()
