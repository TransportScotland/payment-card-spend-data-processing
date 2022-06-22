# quick script to profile the performance of a python3 script (how long each line takes)
if [ -z "$1" ]; then 
    echo "Usage: ./profile.sh file_to_profile.py"
else
    python3 -m cProfile -s cumtime $1 | (head -40; echo ...; tail -5)
fi
