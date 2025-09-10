import base64

class AnalyzeText:
    def __init__(self, high_hostile_words, low_hostile_words):
        self.high_hostile_words = set(base64.b64decode(high_hostile_words).decode('utf-8').split(","))
        self.low_hostile_words = set(base64.b64decode(low_hostile_words).decode('utf-8').split(","))



    def analyze(self,text,threshold=2):
        words = text.split()
        total_words = len(words)
        score = 0

        score += sum(word in self.high_hostile_words for word in words) * 2
        score += sum(word in self.low_hostile_words for word in words)

        risk_percentage = (score / total_words) * 100

        risk_level = "none"
        if risk_percentage >= 2:
            risk_level = "medium"
        if risk_percentage >= 3:
            risk_level = "high"

        is_bds = risk_percentage >= threshold

        return {
            "bds_percent": risk_percentage,
            "is_bds": is_bds,
            "bds_threat_level": risk_level
        }







