import pika
from mongoengine import connect
from bson import ObjectId

connect('contacts_db')



class Contact(Document):
    full_name = StringField(required=True)
    email = StringField(required=True)
    message_sent = BooleanField(default=False)


def send_email_contact(contact_id):
    contact = Contact.objects.get(id=ObjectId(contact_id))
    print(f"Sending email to {contact.email}")
    contact.message_sent = True
    contact.save()
    print(f"Email sent to {contact.email}. Contact marked as message sent.")


def callback(ch, method, properties, body):
    message = body.decode('utf-8')
    contact_id = json.loads(message)['contact_id']
    send_email_contact(contact_id)
    print(f"Received {message} from the email_queue")
    ch.basic_ack(delivery_tag=method.delivery_tag)


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='email_queue')
channel.basic_consume(queue='email_queue', on_message_callback=callback)

print('Consumer is waiting for messages. To exit press CTRL+C')
channel.start_consuming()
