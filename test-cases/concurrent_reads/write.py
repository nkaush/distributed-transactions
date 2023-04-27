BALANCES = {
    "A.a": 10,
    "A.b": 11,
    "A.c": 12,
    "B.a": 20,
    "B.b": 21,
    "B.c": 22,
    "C.a": 30,
    "C.b": 31,
    "C.c": 32,
    "D.a": 40,
    "D.b": 41,
    "D.c": 42,
    "E.a": 50,
    "E.b": 51,
    "E.c": 52,
}

if __name__ == "__main__":
    print("BEGIN")

    for acct in BALANCES:
        print(f"DEPOSIT {acct} {BALANCES[acct]}")

    print("COMMIT")