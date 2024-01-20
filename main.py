import pandas as pd
from typing import Dict, Any
import os
import pyaudio  
import wave
import datetime
from tqdm import tqdm

CSV_FILE = 'data/tira-codeswitching.csv'
CLIP_DIR = 'data/clips'

def main() -> int:
    df = pd.read_csv(CSV_FILE, keep_default_na=False)
    not_processed = df['processed']==''

    for i, row in tqdm(df[not_processed].iterrows(), total=len(df[not_processed])):
        response = process_row(row)
        if response == 'exit':
            exit(1)
        if response:
            df.at[i, 'new_transcription'] = response
            df.at[i, 'code_switched'] = True
        else:
            df.at[i, 'new_transcription'] = row['original_transcription']
            df.at[i, 'code_switched'] = False
        df.at[i, 'processed'] = True
        df.at[i, 'timestamp'] = str(datetime.datetime.now())
        df.to_csv(CSV_FILE, index=False)
    return 0

def process_row(row: Dict[str, Any]) -> Dict[str, Any]:
    transcription = row['original_transcription']
    clipfile = row['clip_name']
    clip_path = os.path.join(CLIP_DIR, clipfile)
    print("Transcription:", transcription)
    play_audio(clip_path)
    response = input(
        "Does this audio contain code switching?\n"+\
        "If so, type the English words missing from the transcription "+\
        "and use `1` to represent the original Tira.\n"+\
        "For example `yeah that's 1` or `1 sounds better`\n"+\
        "Simply press Enter if there is no code switching in the audio.\n"+\
        "You may type `replay` to play the audio again.\n"+\
        "You may type `exit` to quit (progress saves after every record).\n"+\
        "Type your response here:"
    ).lower()
    if (not response) or (response) == 'exit':
        return response
    elif response=='replay':
        play_audio(clip_path)
    while '1' not in response:
        response = input(
            "Response must contain `1` (indicating where Tira is relative to "+\
            "English, be nothing (by just pressing enter), or be `exit`."
        ).lower()
        if (not response) or (response) == 'exit':
            return response
        if response=='replay':
            play_audio(clip_path)

    split_across_1 = response.split('1')
    english_strs = [f' ({word.strip()}) ' if word else '' for word in split_across_1]
    response = transcription.join(english_strs)
    return response.strip()
    
def play_audio(audio_file: str) -> None:
    # taken from user zhangyangyu on https://stackoverflow.com/questions/17657103/play-wav-file-in-python
    #define stream chunk   
    chunk = 1024 
    
    #open a wav format music  
    f = wave.open(audio_file,"rb")  
    #instantiate PyAudio  
    p = pyaudio.PyAudio()  
    #open stream  
    stream = p.open(format = p.get_format_from_width(f.getsampwidth()),  
                    channels = f.getnchannels(),  
                    rate = f.getframerate(),  
                    output = True)  
    #read data  
    data = f.readframes(chunk)  
    
    #play stream  
    while data:  
        stream.write(data)  
        data = f.readframes(chunk)  
    
    #stop stream  
    stream.stop_stream()  
    stream.close()  
    
    #close PyAudio  
    p.terminate()  

if __name__ == '__main__':
    main()