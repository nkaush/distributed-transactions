from write import BALANCES
from time import sleep
import random

print("BEGIN")
accounts = list(BALANCES.keys())

for _ in range(1000):
    acct = random.choice(accounts)
    print(f"BALANCE {acct}")
    # sleep(0.05)

print("COMMIT")