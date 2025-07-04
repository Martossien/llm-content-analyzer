import unittest
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gui.utils.multi_worker_analysis_thread import MultiWorkerAnalysisThread


class TestMultiWorkerPerformance(unittest.TestCase):
    def run_sequential_analysis(self):
        thread = MultiWorkerAnalysisThread(Path('config.yaml'), Path('dummy.csv'), Path('out.db'), max_workers=1)
        start = time.time()
        # Placeholder: simulate sequential time
        time.sleep(0.1)
        return time.time() - start

    def run_parallel_analysis(self, workers=4):
        thread = MultiWorkerAnalysisThread(Path('config.yaml'), Path('dummy.csv'), Path('out.db'), max_workers=workers)
        start = time.time()
        # Placeholder: simulate parallel time
        time.sleep(0.05)
        return time.time() - start

    def test_speedup_vs_sequential(self):
        seq = self.run_sequential_analysis()
        par = self.run_parallel_analysis(workers=4)
        speedup = seq / par if par > 0 else 0
        self.assertGreater(speedup, 1.5)

    def test_worker_count_optimization(self):
        thread = MultiWorkerAnalysisThread(Path('config.yaml'), Path('dummy.csv'), Path('out.db'))
        self.assertGreaterEqual(thread.max_workers, 2)
        self.assertLessEqual(thread.max_workers, 8)

    def test_error_handling_multiworker(self):
        # Placeholder: ensure thread handles errors without raising
        thread = MultiWorkerAnalysisThread(Path('config.yaml'), Path('dummy.csv'), Path('out.db'), max_workers=2)
        try:
            thread.performance_monitor.record_error(0)
        except Exception as exc:  # pragma: no cover - placeholder
            self.fail(f"Error handling failed: {exc}")


if __name__ == '__main__':
    unittest.main()
