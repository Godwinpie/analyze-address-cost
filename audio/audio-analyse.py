import os
import speech_recognition as sr
from pydub import AudioSegment
from transformers import pipeline
from pyannote.audio import Pipeline


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATIC = os.path.join(BASE_DIR, "static")


# Preprocess Audio
def preprocess_audio(audio_file):
    audio = AudioSegment.from_file(audio_file)
    audio = audio.set_frame_rate(16000)
    audio.export("converted_audio.wav", format="wav")
    return "converted_audio.wav"

# Speaker Diarization
def diarize_audio(audio_file):
    # Load pre-trained pipeline from pyannote
    pipeline = Pipeline.from_pretrained("pyannote/speaker-diarization")
    diarization = pipeline({"audio": audio_file})
    return diarization

# Transcribe Audio Segment
def transcribe_audio_segment(audio_file, start_time, end_time):
    recognizer = sr.Recognizer()
    with sr.AudioFile(audio_file) as source:
        audio_data = recognizer.record(source, offset=start_time, duration=end_time - start_time)
        try:
            text = recognizer.recognize_google(audio_data)
        except sr.UnknownValueError:
            text = ""
    return text

# Sentiment Analysis
def analyze_sentiment(text):
    sentiment_pipeline = pipeline("sentiment-analysis")
    sentiments = sentiment_pipeline(text)
    return sentiments

# Putting It All Together
def sentiment_analysis_on_audio(audio_file):
    # Step 1: Preprocess Audio
    processed_audio = preprocess_audio(audio_file)
    
    # Step 2: Diarize Audio
    diarization = diarize_audio(processed_audio)
    
    results = []
    
    # Step 3: Transcribe and Analyze Sentiment for Each Segment
    for turn, _, speaker in diarization.itertracks(yield_label=True):
        start_time, end_time = turn.start, turn.end
        transcription = transcribe_audio_segment(processed_audio, start_time, end_time)
        
        if transcription:
            sentiment_results = analyze_sentiment(transcription)
            results.append({
                "start_time": start_time,
                "end_time": end_time,
                "speaker": speaker,
                "transcription": transcription,
                "sentiment": sentiment_results
            })
    
    return results


if __name__ == "__main__":
    audio_file = open(os.path.join(STATIC, "Lydia call.wav"), "rb")
    sentiment_results = sentiment_analysis_on_audio(audio_file)
    for result in sentiment_results:
        print(f"Time: {result['start_time']} - {result['end_time']}")
        print(f"Speaker: {result['speaker']}")
        print(f"Transcription: {result['transcription']}")
        print(f"Sentiment: {result['sentiment']}")
        print("-" * 30)

