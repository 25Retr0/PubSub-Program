from typing import List

class SubscriptionManager:

    def __init__(self):
        self._subscriptions = []

    def get_subscriptions(self) -> List:
        return list(self._subscriptions)

    def add_subscription(self, topic, filter_criteria=None) -> bool:
        sub = Subscription(topic)
        if filter_criteria:
            sub.add_filter(filter_criteria)

        for other in self._subscriptions:
            if sub == other:
                return False

        self._subscriptions.append(sub)
        return True

    def remove_subscription(self, topic) -> bool:
        for i, sub in enumerate(self._subscriptions):
            if sub.topic == topic:
                self._subscriptions.pop(i)
                return True

        return False


class Subscription:
    def __init__(self, topic: str, op = "", arg = ""):
        self.topic = topic
        self.op = op
        self.arg = arg
        self.filter = ""

        self.valid_ops = ["<", "<=", ">", ">=", "==", "!="]

    def __eq__(self, other):
        return self.topic == other.topic and self.op == other.op and self.arg == other.arg

    def __str__(self):
        topic = self.topic
        if " " in topic:
            topic = f"\"{topic}\""

        if self.filter != "":
            if " " in self.filter:
                filter = f"\"{self.filter}\""
            else:
                filter = self.filter
            return f"/subscribe {topic} {filter}"
        else:
            return f"/subscribe {topic}"

    def get_topic(self):
        return self.topic

    def add_filter(self, filter) -> bool:
        self.filter = filter

        # checking filter, make a operator list that holds all tokens until a non operator is found
        operator_tokens = []
        op_val_split_idx = 0

        valid_ops = ["<", ">", "=", "!"]
        for i, c in enumerate(filter):
            if c in valid_ops:
                operator_tokens.append(c)
            else:
                op_val_split_idx = i
                break;

        op = "".join(operator_tokens)
        if op not in self.valid_ops:
            return False

        value = filter[op_val_split_idx:]
        try:
            value = float(value)
        except Exception:
            return False

        self.op = op
        self.arg = value
        return True
