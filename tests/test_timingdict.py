
import unittest
import time

import lsst.dax.data_generator.timingdict as td



class TestTimingDict(unittest.TestCase):

    def testAddIncrement(self):
        timing = td.TimingDict()
        keya = "a"
        valsa = (270.45, 0.0012, 31, 43)
        suma = 0.0

        keyb = "werf43"
        valsb = (0.92, 10, 0, 43)
        sumb = 0.0

        keyc = "qqc"
        valsc = (75.4, 60.8, 53.2, 87.111)
        sumc = 0.0

        keyd = "bhgde"
        valsd = (0.0, 0.0, 0.0, 0)
        sumd = 0.0
        for j in range(4):
            timing.add(keya, valsa[j])
            suma += valsa[j]
            timing.add(keyb, valsb[j])
            sumb += valsb[j]
            timing.add(keyc, valsc[j])
            sumc += valsc[j]
            timing.add(keyd, valsd[j])
            sumd += valsd[j]
            timing.increment()

        self.assertEqual(suma, timing.times[keya])
        self.assertEqual(sumb, timing.times[keyb])
        self.assertEqual(sumc, timing.times[keyc])
        self.assertEqual(sumd, timing.times[keyd])
        self.assertEqual(timing.count, 4)

        timing_b = td.TimingDict()
        self.assertTrue(timing != timing_b)
        timing_b.combine(timing)
        self.assertTrue(timing == timing_b)
        timing_b.increment()
        self.assertTrue(timing != timing_b)
        timing_b = td.TimingDict()
        timing_b.combine(timing)
        self.assertTrue(timing == timing_b)
        timing_b.add(keyc, 0.0001)
        self.assertTrue(timing != timing_b)

        t_2 = td.TimingDict()
        val = 0.2
        t_2.add(keya, val)
        suma += val
        val = 5
        t_2.add(keyb, val)
        sumb += val
        val = 0.743
        t_2.add(keyc, val)
        sumc += val
        val = 418.4
        t_2.add(keyd, val)
        sumd += val
        t_2.increment()

        timing.combine(t_2)
        self.assertEqual(suma, timing.times[keya])
        self.assertEqual(sumb, timing.times[keyb])
        self.assertEqual(sumc, timing.times[keyc])
        self.assertEqual(sumd, timing.times[keyd])
        self.assertEqual(timing.count, 5)

        # Check that full report doesn't crash
        print(timing.report())

    def testStartEndReset(self):
        timing = td.TimingDict()
        # test empty report doesn't crash
        print(timing.report())
        st = timing.start()
        time.sleep(0.01)
        timing.end("a", st)
        self.assertIn("a", timing.times)
        self.assertTrue(timing.times["a"] > 0.0)


