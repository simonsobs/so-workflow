import numpy as np

# Define some telescope scanning patterns
def scanwave(x0, v_scan, dt, turntime, halfscan, start=False, end=False):
    """
    Generate a single scan waveform
    """
    hscantime = halfscan / v_scan  # scan time in each direction

    v = v_scan * np.ones(int(hscantime/dt))
    v = np.append(v, np.linspace(v_scan, -v_scan, int(turntime/dt)))
    v = np.append(v, -v_scan * np.ones(2*int(hscantime/dt)))
    v = np.append(v, np.linspace(-v_scan, v_scan, int(turntime/dt)))
    v = np.append(v, v_scan * np.ones(int(hscantime/dt)))

    # Accelerate from 0 velocity
    if start:
        v = np.append(np.linspace(0, v_scan, int(turntime/2/dt)), v[int(turntime/2/dt/2):])
    # Decelerate to 0 velocity
    if end:
        v = np.append(v[:-int(turntime/2/dt/2)], np.linspace(v_scan, 0, int(turntime/2/dt)))

    x = x0 + np.cumsum(v) * dt
    return x

def slew(v_slew, dt, start_pos, end_pos):
    """
    Generate a slew movement
    """
    v_slew *= np.sign(end_pos - start_pos)   # need to account for direction
    ntotal = np.abs(end_pos - start_pos)/dt  # total samples during slew

    v = v_slew * np.ones(int(ntotal))
    x = start_pos + np.cumsum(v) * dt
    return x
