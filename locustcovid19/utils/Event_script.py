import pandas as pd

class EventType:

    def __init__(self, file_path, events_dict_path):
        self.file_path = file_path
        self.events_dict_path = events_dict_path
        self.append()

    def get_new_events(self, file_path):
        df_events = pd.read_excel(file_path)
        df_events = df_events['EVENT_TYPE'].unique()
        return set(df_events.tolist())

    def get_old_events(self, file_path):
        df_events = pd.read_csv(file_path, sep=',')
        df_events = df_events['event_type'].unique()
        return set(df_events.tolist())

    def compare(self):
        new_events = self.get_new_events(self.file_path)
        old_events = self.get_old_events(self.events_dict_path)
        self.diff = new_events-old_events

    def append(self):
        self.compare()
        self.df_dict = pd.read_csv(self.events_dict_path, sep=',')
        max_id = self.df_dict['eventID'].max()
        for elem in list(self.diff):
            max_id += 1
            self.df_dict = self.df_dict.append({'eventID':max_id, 'event_type':elem}, ignore_index=True)
        self.df_dict.to_csv(self.events_dict_path, index=False)

new_events=EventType('Africa_1997-2020_Jun13.xlsx', 'events.csv')