import math

class Normalizer:
    @staticmethod
    def min_max(scores):
        min_score = min(scores)
        max_score = max(scores)
        if max_score == min_score:
            return [1.0] * len(scores)
        return [(score - min_score) / (max_score - min_score) for score in scores]

    @staticmethod
    def z_score(scores):
        mean_score = sum(scores) / len(scores)
        std_dev = (sum((score - mean_score) ** 2 for score in scores) / len(scores)) ** 0.5
        if std_dev == 0:
            return [0.0] * len(scores)
        return [(score - mean_score) / std_dev for score in scores]

    @staticmethod
    def log_transform(scores):
        return [math.log(score + 1) for score in scores]  # Adding 1 to handle zero scores

    @staticmethod
    def robust_scale(scores):
        sorted_scores = sorted(scores)
        median = sorted_scores[len(scores) // 2]
        q1 = sorted_scores[len(scores) // 4]
        q3 = sorted_scores[3 * len(scores) // 4]
        iqr = q3 - q1
        if iqr == 0:
            return [0.0] * len(scores)
        return [(score - median) / iqr for score in scores]

    @staticmethod
    def clip_scores(scores, min_val, max_val):
        return [max(min(score, max_val), min_val) for score in scores]

    @staticmethod
    def exp_transform(scores):
        return [1 - math.exp(-score) for score in scores]


# Example usage:
if __name__ == "__main__":
    scores = [10, 20, 30, 40, 1000]
    normalizer = Normalizer()

    min_max_normalized_scores = normalizer.min_max(scores)
    print("Min-Max Normalized Scores:", min_max_normalized_scores)

    z_score_normalized_scores = normalizer.z_score(scores)
    print("Z-Score Normalized Scores:", z_score_normalized_scores)

    log_transformed_scores = normalizer.log_transform(scores)
    print("Log Transformed Scores:", log_transformed_scores)

    robust_scaled_scores = normalizer.robust_scale(scores)
    print("Robust Scaled Scores:", robust_scaled_scores)

    clipped_scores = normalizer.clip_scores(scores, 0, 50)
    print("Clipped Scores:", clipped_scores)

    exp_transformed_scores = normalizer.exp_transform(scores)
    print("Exponential Transformed Scores:", exp_transformed_scores)
