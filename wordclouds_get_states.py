import os

wordcloud_dir = '/mnt/wordclouds'
wordcloud_states_file = '/mnt/wordclouds/states.txt'

def update_states_file():
    with open(wordcloud_states_file, 'w+') as f:
        [f.write('%s\n' % state) for state in os.listdir(wordcloud_dir) if ( os.path.isdir(os.path.join(wordcloud_dir, state)) and state != '1')]

if __name__ == '__main__':
    update_states_file()
