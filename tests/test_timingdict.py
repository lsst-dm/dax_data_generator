
import unittest
import time

import lsst.dax.data_generator.timingdict as td



class TestTimingDict(unittest.TestCase):

    def testCreate(self):
        timing = td.TimingDict()
        timing.add("t", 387.32)
        timing.increment()

        timing_dict = timing.timing_dict.copy()
        t_2 = td.TimingDict(timing.timing_dict)
        self.assertEqual(t_2.timing_dict, timing.timing_dict)

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

        self.assertEqual(suma, timing.timing_dict[keya])
        self.assertEqual(sumb, timing.timing_dict[keyb])
        self.assertEqual(sumc, timing.timing_dict[keyc])
        self.assertEqual(sumd, timing.timing_dict[keyd])
        self.assertEqual(timing.timing_dict["count"], 4.0)

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
        self.assertEqual(suma, timing.timing_dict[keya])
        self.assertEqual(sumb, timing.timing_dict[keyb])
        self.assertEqual(sumc, timing.timing_dict[keyc])
        self.assertEqual(sumd, timing.timing_dict[keyd])
        self.assertEqual(timing.timing_dict["count"], 5.0)

        # Check that full report doesn't crash
        print(timing.report())

    def testStartEndReset(self):
        timing = td.TimingDict()
        # test empty report doesn't crash
        print(timing.report())
        st = timing.start()
        time.sleep(0.01)
        timing.end("a", st)
        self.assertIn("a", timing.timing_dict)
        self.assertTrue(timing.timing_dict["a"] > 0.0)

        timing.reset()
        self.assertTrue(not timing.timing_dict)

