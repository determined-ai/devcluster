import time

import yaml

import devcluster

def test_api():
    config_text = """
        stages:
          - custom:
              name: db
              post:
                - logcheck:
                    regex: "db up"
              cmd:
                # Note: we could use sh here, but it exhibits weird behavior when we try to kill
                # it, but only in some environments.  Python always does the right thing.
                - python
                - -c
                - "import time; time.sleep(1); print('db up'); time.sleep(30)"

          - custom:
              name: master
              pre:
                - sh: "echo 'hi'; sleep 1"
              post:
                - logcheck:
                    regex: "master up"
              cmd:
                # Again, prefer python -c for better responses to signals.
                - python
                - -c
                - "import time; time.sleep(1); print('master up'); time.sleep(30)"

          - custom:
              name: agent
              cmd:
                # Again, prefer python -c for better responses to signals.
                - python
                - -c
                - "import time; print('I am a python agent!'); time.sleep(30)"
    """
    config = yaml.safe_load(config_text)

    with devcluster.Devcluster(config=config) as dc:
        print("up!")
        dc.set_target(3, wait=True, timeout=10)
        print("stage 3!")
        dc.set_target(0, wait=True, timeout=10)
        print("stage 0!")

        try:
            dc.set_target("db", wait=True, timeout=0.0000001)
            raise ValueError("hm... suspiciously fast...")
        except TimeoutError:
            print("the timeout was to be expected")

        with dc.wait_for_stage_log("master", b"master up"):
            dc.set_target("master", wait=False)
        print("master ready!")

        # set_target(wait=True) should is basically a noop if we're already there
        dc.set_target("master", wait=True, timeout=10)
        dc.set_target("master", wait=True, timeout=0.0000001)
        dc.set_target("master", wait=True, timeout=0.0000001)

        time.sleep(1)
        print("killing master and agent!")
        dc.kill_stage(2)
        dc.kill_stage("agent")
        time.sleep(1)
        print("restarting master and agent!")
        dc.restart_stage(2)
        dc.restart_stage("agent")
        time.sleep(1)

    print("dn!")

if __name__ == "__main__":
    test_api()
