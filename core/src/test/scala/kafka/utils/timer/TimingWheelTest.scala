package kafka.utils.timer

import org.scalatest.FunSuite

/**
 * @author zhenchao.wang 2019-04-27 16:25
 * @version 1.0.0
 */
class TimingWheelTest extends FunSuite {

    test("testAdvanceClock") {

    }

}

class MyTimeTask(override val delayMs: Long) extends TimerTask {

    override def run(): Unit = {

    }

}
