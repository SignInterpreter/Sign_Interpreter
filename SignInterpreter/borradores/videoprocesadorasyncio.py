#import sys
#sys.path.append("../")
from streamlit_webrtc import webrtc_streamer, WebRtcMode,VideoProcessorBase
import av
import cv2
import numpy as np
import queue
from interprete.producer.productor import  send
from data.keypoints import mp_holistic
from strem.rtc_config import RTC_CONFIGURATION
from aiortc.contrib.media import MediaPlayer
from predict import  update_cv2
import queue
import asyncio




def consume_kafka_results():
    pass
    # consume mensajes del topic de kafka donde se emiten resultados y actualiza la variable global

#global kafka_prediciton = "continua"


class VideoProcessor(VideoProcessorBase):
    
    def __init__(self):
       # self.model = load_model("action.h5")
        self.sequence = []
        self.predict_threshold = 50
        self.frame_count = 0
        self.label = ""
        self.delay = 1.0
        #self.holograma = mp_holistic.Holistic(min_detection_confidence=0.5, min_tracking_confidence=0.5)
        #self.kafka = send(self.sequence,self.holograma)
        

    def recv(self, frame: av.VideoFrame) -> av.VideoFrame:
        
       with mp_holistic.Holistic(min_detection_confidence=0.5, min_tracking_confidence=0.5) as holistic:
            img =  frame.to_ndarray(format="bgr24")
            #update_cv2(img,self.label)
            self.sequence.append(extraerkeypoints(img,holistic))
            self.sequence = self.sequence[-30:]
            
            if self.frame_count > self.predict_threshold:
                self.frame_count = 0
                #enviamos las imagenes al modelo pra predecir  
                self.label = "calculando"
                print("calculando")
                #update_cv2(img,self.label)                                      
                self.pronostico = predice(self.sequence, holistic)
                print(self.pronostico)
                print(type(self.pronostico))
                self.label = self.pronostico            
                #update_cv2(img,self.label)
                print(str(self.label))
                self.label = juego(str(self.pronostico))
                print(self.label)
                #update_cv2(img,self.label)
                    # Send self.sequence to kafka topic
            else:
                self.frame_count += 1
                
                
                #consumer kafka de cada 24 frames consume 1
                # kaka actualiza la variable  global 
            
            # with opencv write text on global variable ka_fa_predicion in image
            

        return av.VideoFrame.from_ndarray(img, format="bgr24")

webrtc_ctx =webrtc_streamer(
    key="Sign_Interpreter",
    mode=WebRtcMode.SENDRECV,
    #rtc_configuration=RTC_CONFIGURATION,
    video_processor_factory=VideoProcessor,
    media_stream_constraints={"video": True, "audio": False},
    async_processing=True,
    )
   