import numpy as np

from app.services.modeling.probability_estimator import LogisticProbabilityModel


def test_logistic_probability_model_learns_probability_ordering():
    feature_matrix = np.array(
        [
            [-2.0, -1.0],
            [-1.0, -0.5],
            [1.0, 0.5],
            [2.0, 1.0],
        ],
        dtype=float,
    )
    labels = np.array([0.0, 0.0, 1.0, 1.0], dtype=float)

    model = LogisticProbabilityModel.fit(
        feature_matrix,
        labels,
        feature_names=("x1", "x2"),
        learning_rate=0.20,
        iterations=600,
        l2_penalty=0.0001,
    )

    low_probability = model.predict_probability(np.array([-1.5, -0.75], dtype=float))
    high_probability = model.predict_probability(np.array([1.5, 0.75], dtype=float))

    assert 0.0 < low_probability < 1.0
    assert 0.0 < high_probability < 1.0
    assert high_probability > low_probability
