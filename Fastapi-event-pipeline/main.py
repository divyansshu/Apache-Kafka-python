from fastapi import FastAPI, Request, BackgroundTasks
from pydantic import BaseModel
from contextlib import asynccontextmanager
from confluent_kafka import Producer


# -------------configuration------------
config = {
    'bootstrap.servers': 'localhost:9092'
}
TOPIC = 'Events'

#------------lifespan---------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # create the producer
    app.state.producer = Producer(config)
    print("[STARTUP] kafka producer ready..")
    yield
    app.state.producer.flush()
    print("[SHUTDOWN] kafka producer flushed and closed..")

#------------------delivery callback-------------
def delivery_callback(err, msg):
    if err:
        print(f'Delivery Failed: {err}')
    else:
        print(f'[OK] topic={msg.topic()}\n'
              f'partition={msg.partition()}\n'
              f'offset={msg.offset()}\n'
              f'key={msg.key().decode()}\n'
        )

#----------app----------
app = FastAPI(lifespan=lifespan)

#-------schema-------------
class Event(BaseModel):
    user_id: int
    action: str
    ip_address: str
    timestamp: float

#-----------helper--------------
def _produce(producer: Producer, event: Event)-> None:
    """serialize and send one event to kafka. Runs in background"""
    producer.produce(
        topic=TOPIC,
        key=str(event.user_id).encode('utf-8'),
        value=event.model_dump_json().encode('utf-8'),
        callback=delivery_callback
    )
    producer.poll(0) # triggers delivery callback for already sent messages


#----------routes-------------

@app.get('/health')
async def get_health():
    return {"status": "OK"}

@app.post('/event', status_code=202)
async def receive_event(event: Event, request: Request, background_tasks: BackgroundTasks):
    producer = request.app.state.producer
    background_tasks.add_task(_produce, producer, event)
    return {'status': 'accepted', 'user_id': event.user_id}

